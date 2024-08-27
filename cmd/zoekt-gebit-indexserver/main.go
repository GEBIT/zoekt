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
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/build"
	"github.com/sourcegraph/zoekt/cmd"
	"github.com/sourcegraph/zoekt/ctags"
	"github.com/sourcegraph/zoekt/gitindex"

	"github.com/fsnotify/fsnotify"
)

const (
	INDEX_PERIOD_S         = 60
	WATCH_REFRESH_PERIOD_S = 60
	ORPHAN_CHECK_PERIOD_S  = 300
)

type indexRequest struct {
	RepoDir     string `json:"repoDir,omitempty"`
}

type indexAllRequest struct {
	Incremental bool `json:"incremental,omitempty"`
}

var (
	markedForIndex    = map[string]bool{}
	indexRunning      = map[string]bool{}
	gitRepos          = map[string]string{}
	gitReposMutex     sync.Mutex
	globalGitOpts     gitindex.Options
	globalBuildOpts   build.Options
	globalWatcher     *fsnotify.Watcher
	globalRootRepoDir string
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

func startIndexingApi(listen string) {
	http.HandleFunc("/index", serveIndex)
	http.HandleFunc("/index-all", serveIndexAll)
	http.HandleFunc("/reload-repos", serveReloadRepos)
	http.HandleFunc("/list-repos", serveListRepos)

	if err := http.ListenAndServe(listen, nil); err != nil {
		log.Fatal(err)
	}
}

// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  http://localhost:6060/list-repos
func serveListRepos(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		respondWithError(w, errors.New("http method must be GET"))
		return
	}

	gitReposMutex.Lock()
	repoDirs := make([]string, 0, len(gitRepos))
	for k, _ := range gitRepos {
		repoDirs = append(repoDirs, k)
	}
	gitReposMutex.Unlock()

	sort.Strings(repoDirs)
	response := map[string]any{
		"Success": true,
		"RepoDirs": repoDirs,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// example curl:
//
//	curl -X POST --header "Content-Type: application/json" \
//	  http://localhost:6060/reload-repos
func serveReloadRepos(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		respondWithError(w, errors.New("http method must be POST"))
		return
	}

	log.Println("ReloadRepos started")
	gitReposMutex.Lock()

	// clean gitRepos map
	gitRepos = map[string]string{}

	// add git repos again by file walking
	err := filepath.Walk(globalRootRepoDir, addGitRepos)
	if err != nil {
		log.Println(err)
	}

	// create new watcher
	newGlobalWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		respondWithError(w, err)
		return
	}

	// watch repos with new watcher
	go watchRepoDirs(newGlobalWatcher)
	
	// swap global watcher, also closing the old watcher
	oldGlobalWatcher := globalWatcher
	globalWatcher = newGlobalWatcher
	oldGlobalWatcher.Close()
	
	gitReposMutex.Unlock()
	log.Println("ReloadRepos finished")

	response := map[string]any{
		"Success": true,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  --data '{"repoDir":"/r/sparpos/sparpos-kassa.git"}' \
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

	_, ok := markedForIndex[req.RepoDir]
	if !ok {
		respondWithError(w, err)
		return
	}

	log.Printf("received serveIndex request for repoDir: %v", req.RepoDir)

	gitOpts := prepareGitOpts(req.RepoDir)

	markedForIndex[req.RepoDir] = true
	if err := indexRepoWithGitOpts(req.RepoDir, gitOpts); err != nil {
		respondWithError(w, err)
		return
	}

	response := map[string]any{
		"Success": true,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  --data '{"incremental":true}' \
//	  http://localhost:6060/index-all
func serveIndexAll(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req indexAllRequest
	err := dec.Decode(&req)
	if err != nil {
		log.Printf("Error decoding index request: %v", err)
		http.Error(w, "JSON parser error", http.StatusBadRequest)
		return
	}

	log.Printf("received serveIndexAll request with incremental: %v", req.Incremental)

	exitStatus := indexAll(req.Incremental)
	if exitStatus != 0 {
		respondWithError(w, errors.New("error while running indexAll"))
		return
	}

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

func indexRepoWithGitOpts(repoDir string, gitOpts gitindex.Options) error {
	// mark index running for this repo, prevents additional go routine starts
	indexRunning[repoDir] = true

	// run while markedForIndex is true, might get set again by a fs event
	for markedForIndex[repoDir] {
		markedForIndex[repoDir] = false
		log.Printf("start IndexGitRepo dir: %v, name %v", repoDir, gitOpts.BuildOptions.RepositoryDescription.Name)
		err := gitindex.IndexGitRepo(gitOpts)
		if err != nil {
			log.Printf("error in IndexGitRepo(%s, delta=%t): %v", repoDir, gitOpts.BuildOptions.IsDelta, err)
			indexRunning[repoDir] = false
			return err
		}
	}
	indexRunning[repoDir] = false
	return nil
}

// create copy of global opts for this index run and set run-specific values
func prepareGitOpts(repoDir string) gitindex.Options {
	opts := globalBuildOpts
	opts.RepositoryDescription.Name = gitRepos[repoDir]
	gitOpts := globalGitOpts
	gitOpts.RepoDir = repoDir
	gitOpts.BuildOptions = opts

	return gitOpts
}

func indexRepo(repoDir string) error {
	return indexRepoWithGitOpts(repoDir, prepareGitOpts(repoDir))
}

func indexAll(incremental bool) int {
	exitStatus := 0
	gitReposMutex.Lock()
	for repoDir := range gitRepos {
		markedForIndex[repoDir] = true
		gitOpts := prepareGitOpts(repoDir)
		gitOpts.Incremental = incremental
		if err := indexRepoWithGitOpts(repoDir, gitOpts); err != nil {
			exitStatus = 1
		}
	}
	gitReposMutex.Unlock()
	return exitStatus
}

func watchRepoDirs(watcher *fsnotify.Watcher) {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			// log.Println("watcher event:", event)
			if event.Has(fsnotify.Remove) {
				repoFile := filepath.Base(event.Name)
				repoDir := filepath.Dir(event.Name)
				// log.Println("removed file:", repoFile)
				if repoFile == "HEAD.lock" {
					log.Printf("push detected for repoDir: %v", repoDir)
					markedForIndex[repoDir] = true
					if !indexRunning[repoDir] {
						log.Println("start index run for repoDir:", repoDir)
						go indexRepo(repoDir)
					} else {
						log.Printf("index for repoDir %v already running, marked again", repoDir)
					}
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Println("error:", err)
		}
	}
}

// periodically refresh watches to catch repos that were
// deleted and then created again
func refreshWatches() {
	t := time.NewTicker(time.Second * WATCH_REFRESH_PERIOD_S)

	for {
		gitReposMutex.Lock()
		refreshAllWatches(globalWatcher)
		gitReposMutex.Unlock()
		<-t.C
	}
}

func refreshAllWatches(watcher *fsnotify.Watcher) {
	for repoDir := range gitRepos {
		watcher.Remove(repoDir)
		watcher.Add(repoDir)
	}
}

func sanitizeRepoDir(repoDir string) string {
	repoDir, err := filepath.Abs(repoDir)
	if err != nil {
		log.Fatal(err)
	}
	return filepath.Clean(repoDir)
}

func addGitRepo(repoDir string, repoCacheDir string) {
	repoDir = sanitizeRepoDir(repoDir)

	name := strings.TrimSuffix(repoDir, "/.git")
	if repoCacheDir != "" && strings.HasPrefix(name, repoCacheDir) {
		name = strings.TrimPrefix(name, repoCacheDir+"/")
		name = strings.TrimSuffix(name, ".git")
	} else {
		name = strings.TrimSuffix(filepath.Base(name), ".git")
	}
	gitRepos[repoDir] = name
}

func addGitRepos(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if ! info.IsDir() {
		addGitRepo(path, globalGitOpts.RepoCacheDir)
	}
	return nil
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

	globalRootRepoDir = *rootRepoDir
	// set initial gitRepos by walking the root repo file tree
	err := filepath.Walk(globalRootRepoDir, addGitRepos)
	if err != nil {
		log.Println(err)
		return 1
	}

	log.Println("gitRepos set")

	go deleteOrphanIndexes(*indexDir)
	log.Println("deleteOrphanIndexes started")

	globalWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer globalWatcher.Close()

	// start watches before inital index, so we catch pushes that happen
	// during the initial index
	go refreshWatches()
	log.Println("refreshWatches started")

	go watchRepoDirs(globalWatcher)
	log.Println("watchRepoDirs started")

	// initial index run
	exitStatus := indexAll(*incremental)

	log.Printf("indexingApi starting on: %v", *listen)
	startIndexingApi(*listen)

	return exitStatus
}

func main() {
	exitStatus := run()
	os.Exit(exitStatus)
}
