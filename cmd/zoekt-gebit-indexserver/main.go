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
	"crypto/sha256"
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
	"strconv"
	"strings"
	"time"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/go-git/go-git/v5"
	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/build"
	"github.com/sourcegraph/zoekt/cmd"
	"github.com/sourcegraph/zoekt/ctags"
	"github.com/sourcegraph/zoekt/gitindex"
)

const (
	ORPHAN_CHECK_PERIOD_S  = 300
	REPOSITORIES_BASE_PATH = "/var/lib/git/@hashed"
)

type indexRequest struct {
	ProjectPathWithNamespace string `json:"ProjectPathWithNamespace,omitempty"`
	ProjectId                int    `json:"ProjectId,omitempty"`
}

var (
	// tracks which repoDirs should be indexed
	markedForIndex = map[string]bool{}

	// tracks for which repoDir an index operation is currently running
	indexRunning = map[string]bool{}

	// global copy of the gitOps to use when using multiple threads
	globalGitOpts gitindex.Options

	// global copy of the buildOpts to use when using multiple threads
	globalBuildOpts build.Options

	// tracks if the initial index run has finished
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

// Returns the current status of the indexer as json.
//
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

// Requests an index operation of the given project.
//
// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  --data '{"ProjectPathWithNamespace":"sparpos/sparpos-kassa", "ProjectId": 1234}' \
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

	projectPathWithNamespace := req.ProjectPathWithNamespace
	projectId := req.ProjectId

	// get the sha256 of the projectId
	h := sha256.New()
	h.Write([]byte(strconv.Itoa(projectId)))
	hexDigest := fmt.Sprintf("%x", h.Sum(nil))

	// calculate repo dir from hexDigest of projecId
	repoDir := fmt.Sprintf("%s/%s/%s/%s.git", REPOSITORIES_BASE_PATH, hexDigest[0:2], hexDigest[2:4], hexDigest)

	log.Printf("received serveIndex request for projectPathWithNamespace: %v, projectId: %v, repoDir: %v",
		projectPathWithNamespace, projectId, repoDir)

	// get copy of current gitOpts for new thread
	gitOpts := prepareGitOpts(repoDir, projectPathWithNamespace)

	_, ok := indexRunning[repoDir]
	if !ok {
		// ensure indexRunning exists
		indexRunning[repoDir] = false
	}

	// mark this repoDir as running
	markedForIndex[repoDir] = true

	// (try to) start new thread for index operation
	go indexRepoWithGitOpts(repoDir, gitOpts)

	response := map[string]any{
		"Success": true,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// Writes an error response for a http request.
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

// Indexes a given repoDir with the given gitOpts
func indexRepoWithGitOpts(repoDir string, gitOpts gitindex.Options) {
	if indexRunning[repoDir] {
		log.Printf("IndexGitRepo dir: %v, name %v already running, skip", repoDir, gitOpts.BuildOptions.RepositoryDescription.Name)
		return
	}

	// mark index running for this repo, prevents additional go routine starts
	indexRunning[repoDir] = true

	// run while markedForIndex is true, might get set again by a REST call
	for markedForIndex[repoDir] {
		markedForIndex[repoDir] = false
		log.Printf("start IndexGitRepo dir: %v, name: %v", repoDir, gitOpts.BuildOptions.RepositoryDescription.Name)
		_, err := gitindex.IndexGitRepo(gitOpts)
		if err != nil {
			log.Printf("error in IndexGitRepo(%s, delta=%t): %v", repoDir, gitOpts.BuildOptions.IsDelta, err)
		}
	}

	// index for this repoDir finished, track it
	indexRunning[repoDir] = false
}

// create copy of global opts for this index run and set run-specific values
func prepareGitOpts(repoDir string, repoName string) gitindex.Options {
	opts := globalBuildOpts
	opts.RepositoryDescription.Name = repoName
	gitOpts := globalGitOpts
	gitOpts.RepoDir = repoDir
	gitOpts.BuildOptions = opts

	return gitOpts
}

// Gets all repoDirs by file-walking the root repo dir
// and indexes each repoDir sequentially
func indexAll(incremental bool, rootRepoDir string) {
	repoDirs := walkRootRepoDir(rootRepoDir)

	for _, repoDir := range repoDirs {
		repo, err := git.PlainOpen(repoDir)
		if err != nil {
			log.Printf("error opening git repo: %v", err)
			continue
		}
		repoConfig, err := repo.Config()
		if err != nil {
			log.Printf("error opening git config for repo: %v", err)
			continue
		}
		gitlabSection := repoConfig.Raw.Section("gitlab")
		repoName := gitlabSection.Options.Get("fullpath")
		if len(repoName) == 0 {
			// empty gitlab name, set hash as fallback for now
			splitRepoDir := strings.Split(repoDir, "/")
			repoName = strings.Replace(splitRepoDir[len(splitRepoDir)-1], ".git", "", -1)
			log.Printf("No gitlab git config for repoDir: %v, falling back to hashed repoName: %v", repoDir, repoName)
		}
		gitOpts := prepareGitOpts(repoDir, repoName)
		gitOpts.Incremental = incremental
		markedForIndex[repoDir] = true
		indexRepoWithGitOpts(repoDir, gitOpts)
	}
	initialIndexFinished = true
}

// Cleans up a given repoDir path.
func sanitizeRepoDir(repoDir string) string {
	repoDir, err := filepath.Abs(repoDir)
	if err != nil {
		log.Fatal(err)
	}
	return filepath.Clean(repoDir)
}

// File-walks the root repo dir and collects all symlinks (non-directories).
// Returns a string slice containting all found repoDirs (ordered lexically).
func walkRootRepoDir(rootRepoDir string) []string {
	gitRepos := []string{}

	log.Printf("start walkRootRepoDir at dir: %v", rootRepoDir)

	// get gitRepos by walking the root repo file tree
	err := filepath.Walk(rootRepoDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && strings.HasSuffix(path, ".git") {
			// this is a directory ending in .git, collect it
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
	rootRepoDir := flag.String("root_repo_dir", REPOSITORIES_BASE_PATH, "path to the root directory of all repos")
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

	log.Println("deleteOrphanIndexes starting")
	go deleteOrphanIndexes(*indexDir)

	// initial index run in the background
	log.Println("initialIndex starting")
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
