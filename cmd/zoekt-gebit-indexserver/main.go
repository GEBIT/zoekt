// Copyright 2016 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

////////////////////////////////////////////////////
// BASE ORIGINALLY TAKEN FROM cmd/zoekt-git-index/main.go
////////////////////////////////////////////////////

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/build"
	"github.com/sourcegraph/zoekt/cmd"
	"github.com/sourcegraph/zoekt/ctags"
	"github.com/sourcegraph/zoekt/gitindex"
)

const (
	INDEX_PERIOD_S         = 60
	WATCH_REFRESH_PERIOD_S = 60
	ORPHAN_CHECK_PERIOD_S  = 300
)

type indexRequest struct {
	ProjectPathWithNamespace string `json:"ProjectPathWithNamespace,omitempty"`
}

var (
	markedForIndex       = map[string]bool{}
	indexRunning         = map[string]bool{}
	globalGitOpts        gitindex.Options
	globalBuildOpts      build.Options
	initialIndexFinished = false
)

/////////////////////////////////////////////////////////////////////
// delete*Orphan* ORIGINALLY TAKEN FROM cmd/zoekt-indexserver/main.go
/////////////////////////////////////////////////////////////////////

// Delete the shard if its corresponding git repo can't be found.

func deleteIfOrphan(fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return nil
	}
	defer f.Close()

	ifile, err := zoekt.NewIndexFile(f)
	if err != nil {
		return nil
	}
	defer ifile.Close()

	repos, _, err := zoekt.ReadMetadata(ifile)
	if err != nil {
		return nil
	}

	// TODO support compound shards in zoekt-indexserver
	if len(repos) != 1 {
		return nil
	}
	repo := repos[0]

	_, err = os.Stat(repo.Source)
	if os.IsNotExist(err) {
		log.Printf("deleting orphan shard %s; source %q not found", fn, repo.Source)
		// cleanup maps to avoid mem leaks
		_, ok := indexRunning[repo.Source]
		if ok {
			delete(indexRunning, repo.Source)
		}
		_, ok = markedForIndex[repo.Source]
		if ok {
			delete(markedForIndex, repo.Source)
		}
		return os.Remove(fn)
	}

	return err
}

func deleteOrphanIndexes(indexDir string) {
	t := time.NewTicker(time.Second * ORPHAN_CHECK_PERIOD_S)

	expr := indexDir + "/*"
	for {
		fs, err := filepath.Glob(expr)
		if err != nil {
			log.Printf("Glob(%q): %v", expr, err)
		}

		for _, f := range fs {
			if err := deleteIfOrphan(f); err != nil {
				log.Printf("deleteIfOrphan(%q): %v", f, err)
			}
		}
		<-t.C
	}
}

/////////////////////////////////////////////////////////////////////
// *Index* and respondWithError ORIGINALLY TAKEN FROM cmd/zoekt-dynamic-indexserver/main.go
/////////////////////////////////////////////////////////////////////

func startIndexingApi(listen string) error {
	http.HandleFunc("/index", serveIndex)
	http.HandleFunc("/status", serveStatus)

	if err := http.ListenAndServe(listen, nil); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  http://localhost:6060/status
func serveStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		respondWithError(w, errors.New("http method must be GET"))
		return
	}

	runningRepos := []string{}
	for repoDir, isRunning := range indexRunning {
		if isRunning {
			runningRepos = append(runningRepos, repoDir)
		}
	}

	sort.Strings(runningRepos)

	response := map[string]any{
		"Success":              true,
		"InitialIndexFinished": initialIndexFinished,
		"RunningRepos":         runningRepos,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  --data '{"ProjectPathWithNamespace":"sparpos/sparpos-kassa"}' \
//	  http://localhost:6060/index
func serveIndex(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req indexRequest
	err := dec.Decode(&req)
	if err != nil {
		log.Printf("Error decoding index request: %v", err)
		http.Error(w, "JSON parser error", http.StatusBadRequest)
		return
	}

	// calculate repo dir from project path
	repoDir := fmt.Sprintf("/r/%s.git", req.ProjectPathWithNamespace)

	log.Printf("received serveIndex request for repoDir: %v", repoDir)

	gitOpts := prepareGitOpts(repoDir)

	_, ok := indexRunning[repoDir]
	if !ok {
		// ensure indexRunning exists
		indexRunning[repoDir] = false
	}
	markedForIndex[repoDir] = true
	go indexRepoWithGitOpts(repoDir, gitOpts)

	log.Printf("started index for serveIndex request for repoDir: %v", repoDir)

	response := map[string]any{
		"Success": true,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func respondWithError(w http.ResponseWriter, err error) {
	responseCode := http.StatusInternalServerError

	log.Print(err)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseCode)
	response := map[string]any{
		"Success": false,
		"Error":   err.Error(),
	}

	_ = json.NewEncoder(w).Encode(response)
}

func indexRepoWithGitOpts(repoDir string, gitOpts gitindex.Options) {
	if indexRunning[repoDir] {
		log.Printf("IndexGitRepo dir: %v, name %v already running, skip", repoDir, gitOpts.BuildOptions.RepositoryDescription.Name)
		return
	}

	// mark index running for this repo, prevents additional go routine starts
	indexRunning[repoDir] = true

	// run while markedForIndex is true, might get set again by a fs event
	for markedForIndex[repoDir] {
		markedForIndex[repoDir] = false
		log.Printf("start IndexGitRepo dir: %v, name %v", repoDir, gitOpts.BuildOptions.RepositoryDescription.Name)
		_, err := gitindex.IndexGitRepo(gitOpts)
		if err != nil {
			log.Printf("error in IndexGitRepo(%s, delta=%t): %v", repoDir, gitOpts.BuildOptions.IsDelta, err)
		}
	}
	indexRunning[repoDir] = false
}

// create copy of global opts for this index run and set run-specific values
func prepareGitOpts(repoDir string) gitindex.Options {
	opts := globalBuildOpts
	opts.RepositoryDescription.Name = strings.TrimSuffix(strings.TrimPrefix(repoDir, "/r/"), ".git")
	gitOpts := globalGitOpts
	gitOpts.RepoDir = repoDir
	gitOpts.BuildOptions = opts

	return gitOpts
}

func indexAll(incremental bool, rootRepoDir string) {
	repoDirs := walkRootRepoDir(rootRepoDir)

	for _, repoDir := range repoDirs {
		gitOpts := prepareGitOpts(repoDir)
		gitOpts.Incremental = incremental
		markedForIndex[repoDir] = true
		indexRepoWithGitOpts(repoDir, gitOpts)
	}
	initialIndexFinished = true
}

func sanitizeRepoDir(repoDir string) string {
	repoDir, err := filepath.Abs(repoDir)
	if err != nil {
		log.Fatal(err)
	}
	return filepath.Clean(repoDir)
}

func walkRootRepoDir(rootRepoDir string) []string {
	gitRepos := []string{}

	// get gitRepos by walking the root repo file tree
	err := filepath.Walk(rootRepoDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			gitRepos = append(gitRepos, sanitizeRepoDir(path))
		}
		return nil
	})
	if err != nil {
		log.Println(err)
		return nil
	}

	sort.Strings(gitRepos)

	return gitRepos
}

func run() int {
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")

	allowMissing := flag.Bool("allow_missing_branches", false, "allow missing branches.")
	submodules := flag.Bool("submodules", true, "if set to false, do not recurse into submodules")
	branchesStr := flag.String("branches", "HEAD", "git branches to index.")
	branchPrefix := flag.String("prefix", "refs/heads/", "prefix for branch names")

	incremental := flag.Bool("incremental", true, "only index changed repositories")
	repoCacheDir := flag.String("repo_cache", "", "directory holding bare git repos, named by URL. "+
		"this is used to find repositories for submodules. "+
		"It also affects name if the indexed repository is under this directory.")
	isDelta := flag.Bool("delta", false, "whether we should use delta build")
	deltaShardNumberFallbackThreshold := flag.Uint64("delta_threshold", 0, "upper limit on the number of preexisting shards that can exist before attempting a delta build (0 to disable fallback behavior)")
	offlineRanking := flag.String("offline_ranking", "", "the name of the file that contains the ranking info.")
	offlineRankingVersion := flag.String("offline_ranking_version", "", "a version string identifying the contents in offline_ranking.")
	languageMap := flag.String("language_map", "", "a mapping between a language and its ctags processor (a:0,b:3).")
	indexDir := flag.String("index_dir", "", "directory holding index shards. Defaults to $data_dir/index/")
	listen := flag.String("listen", ":6060", "listen on this address.")
	rootRepoDir := flag.String("root_repo_dir", "/r", "path to the root directory of all repos")
	flag.Parse()

	log.Println("flags parsed")
	// Tune GOMAXPROCS to match Linux container CPU quota.
	_, _ = maxprocs.Set()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
		log.Println("cpuprofile created")
	}

	if *repoCacheDir != "" {
		dir, err := filepath.Abs(*repoCacheDir)
		if err != nil {
			log.Fatalf("Abs: %v", err)
		}
		*repoCacheDir = dir
	}
	log.Println("repoCacheDir set")

	globalBuildOpts = *cmd.OptionsFromFlags()
	globalBuildOpts.IsDelta = *isDelta
	globalBuildOpts.DocumentRanksPath = *offlineRanking
	globalBuildOpts.DocumentRanksVersion = *offlineRankingVersion
	globalBuildOpts.IndexDir = *indexDir
	globalBuildOpts.LanguageMap = make(ctags.LanguageMap)
	for _, mapping := range strings.Split(*languageMap, ",") {
		m := strings.Split(mapping, ":")
		if len(m) != 2 {
			continue
		}
		globalBuildOpts.LanguageMap[m[0]] = ctags.StringToParser(m[1])
	}
	log.Println("globalBuildOpts set")

	var branches []string
	if *branchesStr != "" {
		branches = strings.Split(*branchesStr, ",")
	}
	log.Println("branches set")

	globalGitOpts = gitindex.Options{
		BranchPrefix:                      *branchPrefix,
		Incremental:                       *incremental,
		Submodules:                        *submodules,
		RepoCacheDir:                      *repoCacheDir,
		AllowMissingBranch:                *allowMissing,
		BuildOptions:                      build.Options{},
		Branches:                          branches,
		RepoDir:                           "",
		DeltaShardNumberFallbackThreshold: *deltaShardNumberFallbackThreshold,
	}
	log.Println("globalGitOpts set")

	go deleteOrphanIndexes(*indexDir)
	log.Println("deleteOrphanIndexes started")

	// initial index run in the background
	go indexAll(*incremental, *rootRepoDir)

	log.Printf("indexingApi starting on: %v", *listen)
	err := startIndexingApi(*listen)
	exitStatus := 0
	if err != nil {
		exitStatus = 1
	}

	return exitStatus
}

func main() {
	exitStatus := run()
	os.Exit(exitStatus)
}
