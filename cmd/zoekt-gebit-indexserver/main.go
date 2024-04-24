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
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"go.uber.org/automaxprocs/maxprocs"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/cmd"
	"github.com/sourcegraph/zoekt/ctags"
	"github.com/sourcegraph/zoekt/gitindex"

	"github.com/fsnotify/fsnotify"
)

const INDEX_PERIOD_S = 30

type indexRequest struct {
	RepoDir string
}

var markedForIndex = map[string]bool{}

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

func deleteOrphanIndexes(indexDir string, watchInterval time.Duration) {
	t := time.NewTicker(watchInterval)

	expr := indexDir + "/*"
	for {
		log.Print("start orphan check")
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

	log.Printf("start api server on: %v", listen)
	if err := http.ListenAndServe(listen, nil); err != nil {
		log.Fatal(err)
	}
}

// example curl:
//
//	curl --header "Content-Type: application/json" \
//	  --request POST \
//	  --data '{"repoDir":"/home/sourcegraph/till-dev.git"}' \
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
	markedForIndex[req.RepoDir] = true
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
	}

	if *repoCacheDir != "" {
		dir, err := filepath.Abs(*repoCacheDir)
		if err != nil {
			log.Fatalf("Abs: %v", err)
		}
		*repoCacheDir = dir
	}
	log.Println("repoCacheDir set")

	opts := cmd.OptionsFromFlags()
	opts.IsDelta = *isDelta
	opts.DocumentRanksPath = *offlineRanking
	opts.DocumentRanksVersion = *offlineRankingVersion
	opts.IndexDir = *indexDir
	log.Printf("opts: %v", opts)

	var branches []string
	if *branchesStr != "" {
		branches = strings.Split(*branchesStr, ",")
	}

	gitRepos := map[string]string{}
	for _, repoDir := range flag.Args() {
		repoDir, err := filepath.Abs(repoDir)
		if err != nil {
			log.Fatal(err)
		}
		repoDir = filepath.Clean(repoDir)

		name := strings.TrimSuffix(repoDir, "/.git")
		if *repoCacheDir != "" && strings.HasPrefix(name, *repoCacheDir) {
			name = strings.TrimPrefix(name, *repoCacheDir+"/")
			name = strings.TrimSuffix(name, ".git")
		} else {
			name = strings.TrimSuffix(filepath.Base(name), ".git")
		}
		gitRepos[repoDir] = name
	}
	log.Println("gitRepos set")

	opts.LanguageMap = make(ctags.LanguageMap)
	for _, mapping := range strings.Split(*languageMap, ",") {
		m := strings.Split(mapping, ":")
		if len(m) != 2 {
			continue
		}
		opts.LanguageMap[m[0]] = ctags.StringToParser(m[1])
	}
	log.Println("LanguageMap set")

	go deleteOrphanIndexes(*indexDir, 10*time.Second)

	log.Println("deleteOrphanIndexes started")

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	exitStatus := 0
	for repoDir, name := range gitRepos {
		opts.RepositoryDescription.Name = name
		gitOpts := gitindex.Options{
			BranchPrefix:                      *branchPrefix,
			Incremental:                       *incremental,
			Submodules:                        *submodules,
			RepoCacheDir:                      *repoCacheDir,
			AllowMissingBranch:                *allowMissing,
			BuildOptions:                      *opts,
			Branches:                          branches,
			RepoDir:                           repoDir,
			DeltaShardNumberFallbackThreshold: *deltaShardNumberFallbackThreshold,
		}
		log.Printf("start IndexGitRepo dir: %v, name %v", repoDir, opts.RepositoryDescription.Name)
		if err := gitindex.IndexGitRepo(gitOpts); err != nil {
			log.Printf("indexGitRepo(%s, delta=%t): %v", repoDir, gitOpts.BuildOptions.IsDelta, err)
			exitStatus = 1
		}

		log.Println("adding dir to watcher: " + repoDir)
		if err := watcher.Add(repoDir); err != nil {
			log.Fatal(err)
		}
		markedForIndex[repoDir] = false
	}

	// Start listening for fs events.
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("event:", event)
				if event.Has(fsnotify.Remove) {
					repoFile := filepath.Base(event.Name)
					repoDir := filepath.Dir(event.Name)
					log.Println("removed file:", repoFile)
					if repoFile == "HEAD.lock" {
						markedForIndex[repoDir] = true
						log.Println("markedForIndex in watch:", markedForIndex)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	// indexer loop
	go func() {
		for {
			log.Println("markedForIndex in indexer:", markedForIndex)
			for repoDir, marked := range markedForIndex {
				if marked {
					opts.RepositoryDescription.Name = gitRepos[repoDir]
					gitOpts := gitindex.Options{
						BranchPrefix:                      *branchPrefix,
						Incremental:                       *incremental,
						Submodules:                        *submodules,
						RepoCacheDir:                      *repoCacheDir,
						AllowMissingBranch:                *allowMissing,
						BuildOptions:                      *opts,
						Branches:                          branches,
						RepoDir:                           repoDir,
						DeltaShardNumberFallbackThreshold: *deltaShardNumberFallbackThreshold,
					}
					log.Printf("start IndexGitRepo dir: %v, name %v", repoDir, opts.RepositoryDescription.Name)
					if err := gitindex.IndexGitRepo(gitOpts); err != nil {
						log.Printf("indexGitRepo(%s, delta=%t): %v", repoDir, gitOpts.BuildOptions.IsDelta, err)
						exitStatus = 1
					}
					markedForIndex[repoDir] = false
				}
				// re-watch to account for deleted and re-created repoDirs
				watcher.Remove(repoDir)
				watcher.Add(repoDir)
			}
			time.Sleep(time.Second * INDEX_PERIOD_S)
		}
	}()

	// Block main goroutine forever
	//<-make(chan struct{})

	startIndexingApi(*listen)

	return exitStatus
}

func main() {
	exitStatus := run()
	os.Exit(exitStatus)
}
