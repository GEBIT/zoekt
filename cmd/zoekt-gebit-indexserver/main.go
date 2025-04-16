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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/golang-queue/queue/core"

	"github.com/sourcegraph/zoekt/cmd"
	"github.com/sourcegraph/zoekt/internal/ctags"
	"github.com/sourcegraph/zoekt/internal/gitindex"

	"github.com/sourcegraph/zoekt/index"
)

const (
	ORPHAN_CHECK_PERIOD_S  = 300
	REPOSITORIES_BASE_PATH = "/r"
)

type indexAllRequest struct {
	IsIncremental bool
	IsDelta       bool
}

func (j *indexAllRequest) Bytes() []byte {
	return bytes(j)
}

type indexRequest struct {
	indexAllRequest
	IsInitial                bool
	ProjectPathWithNamespace string
}

// Must get implemented a second time, so json.Marshal() picks up
// the correct type and marshals all properties
func (j *indexRequest) Bytes() []byte {
	return bytes(j)
}

func bytes(req any) []byte {
	b, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}
	return b
}

var (
	// global copy of the gitOps to use when using multiple threads
	globalGitOpts gitindex.Options

	// global copy of the buildOpts to use when using multiple threads
	globalBuildOpts index.Options

	// the root dir for the git bare repos to index
	globalRootRepoDir string

	// global reference to worker for getting running index tasks and accessing the queue
	worker *indexWorker

	// start time of initial index
	inititalIndexStartTime time.Time

	// duration of initial index
	inititalIndexDuration time.Duration
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

	ifile, err := index.NewIndexFile(f)
	if err != nil {
		return nil
	}
	defer ifile.Close()

	repos, _, err := index.ReadMetadata(ifile)
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

func deleteOrphanIndexes(indexDir string, watchInterval time.Duration) {
	t := time.NewTicker(watchInterval)

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

func startIndexingApi(listen string) *http.Server {
	server := &http.Server{
		Addr: listen,
	}
	http.HandleFunc("/index", serveIndex)
	http.HandleFunc("/indexAll", serveIndexAll)
	http.HandleFunc("/status", serveStatus)

	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
		log.Println("Stopped serving new connections")
	}()

	return server
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

	numSubmitted := worker.q.SubmittedTasks()
	numCompleted := worker.q.CompletedTasks()
	numQueued := numSubmitted - numCompleted
	numInitialSubmitted := worker.initialMetric.SubmittedTasks()
	numInitialCompleted := worker.initialMetric.CompletedTasks()
	numInitialQueued := numInitialSubmitted - numInitialCompleted
	runningReqs := worker.muIndexDir.Running()

	response := map[string]any{
		"Success":               true,
		"RunningReqs":           runningReqs,
		"BusyWorkers":           worker.q.BusyWorkers(),
		"NumTotalSubmitted":     numSubmitted,
		"NumTotalCompleted":     numCompleted,
		"NumTotalQueued":        numQueued,
		"NumInitialSubmitted":   numInitialSubmitted,
		"NumInitialCompleted":   numInitialCompleted,
		"NumInitialQueued":      numInitialQueued,
		"InititalIndexDuration": fmt.Sprintf("%v", inititalIndexDuration),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// Requests an index operation of the given project.
//
// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  --data '{"IsIncremental": true, "IsDelta": true}' \
//	  http://localhost:6060/indexAll
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
	log.Printf("received valid serveIndexAll request, isInc: %v, isDelta: %v", req.IsIncremental, req.IsDelta)

	go startIndexAll(req.IsIncremental, req.IsDelta, false)

	response := map[string]any{
		"Success": true,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// Requests an index operation of the given project.
//
// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  --data '{"ProjectPathWithNamespace": "gebit-build/gebit-build", "IsIncremental": false, "IsDelta": false}' \
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
	// ensure IsInitial is always false when coming from api
	req.IsInitial = false

	repoDir := calcRepoDir(projectPathWithNamespace)

	errStr, isSkipIndex := checkRepo(repoDir)
	if errStr != "" {
		respondWithError(w, fmt.Errorf("error checking repo: %v", errStr))
		return
	}

	log.Printf("received valid serveIndex request for projectPathWithNamespace: %v, repoDir: %v, isInc: %v, isDelta: %v",
		projectPathWithNamespace, repoDir, req.IsIncremental, req.IsDelta)

	if isSkipIndex {
		log.Printf("skipping index for repoDir %v", repoDir)

		response := map[string]any{
			"Success": true,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)

		return
	}

	err = worker.q.Queue(&req)
	if err != nil {
		log.Printf("error queueing task: %v for indexReq %v", err, req)
		respondWithError(w, fmt.Errorf("error queueing task: %v for indexReq %v", err, req))
		return
	}

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

func calcRepoDir(projectPathWithNamespace string) string {
	return fmt.Sprintf("%s/%s.git", globalRootRepoDir, projectPathWithNamespace)
}

// Indexes a given repoDir with the given gitOpts
func indexRepoWithGitOpts(repoDir string, gitOpts gitindex.Options) {
	start := time.Now()
	log.Printf("indexRepoWithGitOpts start, repoDir: %v, isInc: %v, isDelta: %v",
		repoDir, gitOpts.Incremental, gitOpts.BuildOptions.IsDelta)
	_, err := gitindex.IndexGitRepo(gitOpts)
	if err != nil {
		log.Printf("error in IndexGitRepo(%s, inc=%t, delta=%t): %v",
			repoDir, gitOpts.Incremental, gitOpts.BuildOptions.IsDelta, err)
	}
	duration := time.Since(start)
	log.Printf("indexRepoWithGitOpts finish, repoDir: %v, isInc: %v, isDelta: %v, duration: %v",
		repoDir, gitOpts.Incremental, gitOpts.BuildOptions.IsDelta, duration)
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

func initialIndexFinished() {
	inititalIndexDuration = time.Since(inititalIndexStartTime)
	log.Println("deleteOrphanIndexes starting")
	go deleteOrphanIndexes(globalBuildOpts.IndexDir, time.Second*ORPHAN_CHECK_PERIOD_S)
}

// File-walks the root repo dir and collects directories ending in ".git", but not in ".wiki.git".
// Returns a string slice containting all found repoDirs (ordered lexically).
func walkRootRepoDir(rootRepoDir string) []string {
	start := time.Now()
	gitRepos := []string{}

	log.Printf("walkRootRepoDir start, rootRepoDir: %v", rootRepoDir)

	// get gitRepos by walking the root repo file tree
	err := filepath.Walk(rootRepoDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && !strings.HasSuffix(path, ".wiki.git") && strings.HasSuffix(path, ".git") {
			// this is a non-wiki directory ending in .git, collect it
			gitRepos = append(gitRepos, sanitizeRepoDir(path))
		}
		return nil
	})
	if err != nil {
		log.Println(err)
		return nil
	}

	sort.Strings(gitRepos)

	duration := time.Since(start)
	log.Printf("walkRootRepoDir finish, rootRepoDir: %v, duration: %v", rootRepoDir, duration)
	return gitRepos
}

// Gets all repoDirs by file-walking the root repo dir
// and queues an index job for each repoDir
func startIndexAll(incremental bool, delta bool, initial bool) {
	if initial {
		inititalIndexStartTime = time.Now()
	}

	repoDirs := walkRootRepoDir(globalRootRepoDir)
	for _, repoDir := range repoDirs {
		if _, err := os.Stat(repoDir); errors.Is(err, os.ErrNotExist) {
			// repoDir does not exist, might have been deleted in the meantime
			log.Printf("repoDir %v removed after walkRootRepoDir(), probably deleted, continuing", repoDir)
			continue
		}

		errStr, isSkipIndex := checkRepo(repoDir)
		if errStr != "" {
			// checkRepo() already logged
			continue
		}
		// check if skipindex flag is set
		if isSkipIndex {
			log.Printf("skipping index for repoDir %v", repoDir)
			continue
		}

		projectPathWithNamespace, _ := strings.CutPrefix(repoDir, globalRootRepoDir+"/")
		projectPathWithNamespace, _ = strings.CutSuffix(projectPathWithNamespace, ".git")

		// create index request for repo, ensuring full build
		indexReq := indexRequest{
			ProjectPathWithNamespace: projectPathWithNamespace,
			IsInitial:                initial,
			indexAllRequest: indexAllRequest{
				IsIncremental: incremental,
				IsDelta:       delta,
			},
		}

		err := worker.q.Queue(&indexReq)
		if err != nil {
			log.Printf("error queueing task: %v for indexReq %v", err, indexReq)
		}
	}
}

// Cleans up a given repoDir path.
func sanitizeRepoDir(repoDir string) string {
	repoDir, err := filepath.Abs(repoDir)
	if err != nil {
		log.Fatal(err)
	}
	return filepath.Clean(repoDir)
}

func isSkipIndex(repoConfig *config.Config) bool {
	gebitSection := repoConfig.Raw.Section("gebit")
	skipIndex := gebitSection.Options.Get("skipIndex")
	return skipIndex == "true"
}

func getRepoNameFromConfig(repoConfig *config.Config) string {
	zoektSection := repoConfig.Raw.Section("zoekt")
	return zoektSection.Options.Get("name")
}

// Checks if a repo can and should be indexed.
func checkRepo(repoDir string) (string, bool) {
	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		msg := fmt.Sprintf("error opening git repo: %v", err)
		log.Printf(msg)
		return msg, true
	}
	repoConfig, err := repo.Config()
	if err != nil {
		msg := fmt.Sprintf("error opening git config for repo: %v", err)
		log.Printf(msg)
		return msg, true
	}
	repoName := getRepoNameFromConfig(repoConfig)
	if repoName == "" {
		msg := fmt.Sprintf("repoName is empty for repoDir: %v, repoDir: %v", repoDir, repoDir)
		log.Printf(msg)
		return msg, true
	}

	return "", isSkipIndex(repoConfig)
}

// Callback for the indexWorker, unmarshals a task message into an index request
// and does the actual indexing.
func indexRepoTask(ctx context.Context, m core.TaskMessage) error {
	req, err := UnmarshalReq(m)
	if err != nil {
		return err
	}

	repoDir := calcRepoDir(req.ProjectPathWithNamespace)

	fetchGitRepo(repoDir)

	gitOpts := prepareGitOpts(repoDir, req.ProjectPathWithNamespace)
	gitOpts.Incremental = req.IsIncremental
	gitOpts.BuildOptions.IsDelta = req.IsDelta

	indexRepoWithGitOpts(repoDir, gitOpts)

	return err
}

// fetchGitRepo runs git-fetch, and returns true if there was an
// update.
// originally taken from cmd/zoekt-indexserver.go
func fetchGitRepo(repoDir string) bool {
	start := time.Now()
	log.Printf("fetchGitRepo start, repoDir: %v", repoDir)

	cmd := exec.Command("git", "--git-dir", repoDir, "fetch", "origin",
		"--prune",
		"--no-tags",
		"--depth=1",
		"+refs/heads/*:refs/remotes/origin/*",
		"^refs/heads/feature/*",
		"^refs/heads/deps/*",
		"^refs/heads/user/*",
		"^refs/heads/users/*")

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("command %s failed: %v\nCOMBINED_OUT: %s\n",
			cmd.Args, err, string(output))
		return false
	}
	duration := time.Since(start)
	log.Printf("fetchGitRepo finish, repoDir: %v, duration: %v", repoDir, duration)
	// When fetch found no updates, it prints nothing out
	return len(output) != 0
}

// Checks the repoDirs of all running index tasks if they have left over
// a shallow.lock file. If one is found, it is removed.
func cleanupShallowLocks() {
	runningReqs := worker.muIndexDir.Running()
	for _, req := range runningReqs {
		shallowLock := globalRootRepoDir + "/" + req.ProjectPathWithNamespace + ".git/shallow.lock"
		log.Printf("checking shallow lock %v", shallowLock)
		if _, err := os.Stat(shallowLock); err == nil {
			// shallow lock exists, remove it
			err = os.Remove(shallowLock)
			if err != nil {
				log.Printf("error removing shallow lock %v: %v", shallowLock, err)
			} else {
				log.Printf("found shallow lock %v, removed it", shallowLock)
			}
		}
	}
}

func shutdown(httpServer *http.Server) {
	log.Printf("shutdown sequence initiated")

	// http server shutdown, allow 5 seconds, so we still have 5 seconds before docker uses a kill signal
	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownRelease()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	log.Println("Graceful HTTP shutdown complete")

	// set worker count to 0
	worker.q.UpdateWorkerCount(0)
	// run Release() as a separate go function, so we dont wait for it to finish
	// but no new jobs can start
	go worker.q.Release()
	log.Printf("worker count set to 0 and queue release started in background")

	// cleanup shallow lock files of interrupted git fetches
	cleanupShallowLocks()

	log.Printf("shutdown sequence finished")
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
	languageMap := flag.String("language_map", "", "a mapping between a language and its ctags processor (a:0,b:3).")
	indexDir := flag.String("index_dir", "", "directory holding index shards. Defaults to $data_dir/index/")
	listen := flag.String("listen", ":6060", "listen on this address.")
	rootRepoDir := flag.String("root_repo_dir", REPOSITORIES_BASE_PATH, "path to the root directory of all repos")
	numWorkers := flag.Int64("num_workers", 2, "the number of workers to use for index tasks")
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

	globalRootRepoDir = *rootRepoDir

	globalBuildOpts = *cmd.OptionsFromFlags()
	globalBuildOpts.IsDelta = *isDelta
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
		BuildOptions:                      index.Options{},
		Branches:                          branches,
		RepoDir:                           "",
		DeltaShardNumberFallbackThreshold: *deltaShardNumberFallbackThreshold,
	}
	log.Println("globalGitOpts set")

	log.Println("indexQueue starting")
	worker = NewIndexWorker(*numWorkers, initialIndexFinished)

	log.Println("startIndexAll non-incremental non-delta normal build starting")
	// initial index run
	startIndexAll(false, false, true)

	log.Printf("indexingApi starting on: %v", *listen)
	httpServer := startIndexingApi(*listen)

	defer shutdown(httpServer)

	// make a channel to receivce os signals
	sigChan := make(chan os.Signal, 1)
	// handle SIGINT and SIGTERM, so we can do graceful shutdown
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// block main go routine and wait for signal to arrive
	<-sigChan

	// returning here will run all deferred function calls (especially shutdown())
	return 0
}

func main() {
	exitStatus := run()
	os.Exit(exitStatus)
}
