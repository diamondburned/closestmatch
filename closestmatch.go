package closestmatch

import (
	"compress/gzip"
	"encoding/json"
	"math/rand"
	"os"
	"sort"
	"strings"
)

const letters = "abcdefghijklmnopqrstuvwxyzöäüß"

// ClosestMatch is the structure that contains the
// substring sizes and carrys a map of the substrings for
// easy lookup
type ClosestMatch[Data any] struct {
	SubstringSizes []int
	SubstringToID  map[string]map[uint32]struct{}
	ID             map[uint32]IDInfo[Data]
}

// IDInfo carries the information about the keys
type IDInfo[Data any] struct {
	Key           string
	NumSubstrings int
	Data          Data
}

// New returns a new structure for performing closest matches
func New[Data any](possible map[string]Data, subsetSize []int) *ClosestMatch[Data] {
	cm := new(ClosestMatch[Data])
	cm.SubstringSizes = subsetSize
	cm.SubstringToID = make(map[string]map[uint32]struct{})
	cm.ID = make(map[uint32]IDInfo[Data])
	i := 0
	for k, m := range possible {
		substrings := cm.splitWord(strings.ToLower(k))
		cm.ID[uint32(i)] = IDInfo[Data]{Key: k, NumSubstrings: len(substrings), Data: m}
		for substring := range substrings {
			if _, ok := cm.SubstringToID[substring]; !ok {
				cm.SubstringToID[substring] = make(map[uint32]struct{})
			}
			cm.SubstringToID[substring][uint32(i)] = struct{}{}
		}
		i++
	}

	return cm
}

// Load can load a previously saved ClosestMatch object from disk
func Load[Data any](filename string) (*ClosestMatch[Data], error) {
	cm := new(ClosestMatch[Data])

	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		return cm, err
	}

	w, err := gzip.NewReader(f)
	if err != nil {
		return cm, err
	}

	err = json.NewDecoder(w).Decode(&cm)
	return cm, err
}

// Save writes the current ClosestSave object as a gzipped JSON file
func (cm *ClosestMatch[Data]) Save(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	w := gzip.NewWriter(f)
	defer w.Close()
	enc := json.NewEncoder(w)
	// enc.SetIndent("", " ")
	return enc.Encode(cm)
}

type workerResult[Data any] struct {
	Value int
	Data  Data
}

func (cm *ClosestMatch[Data]) worker(id int, jobs <-chan job, results chan<- result[Data]) {
	for j := range jobs {
		m := make(map[string]workerResult[Data])
		if ids, ok := cm.SubstringToID[j.substring]; ok {
			weight := 1000 / len(ids)
			for id := range ids {
				if _, ok2 := m[cm.ID[id].Key]; !ok2 {
					m[cm.ID[id].Key] = workerResult[Data]{Value: 0, Data: cm.ID[id].Data}
				}
				item := m[cm.ID[id].Key]
				item.Value += 1 + 1000/len(cm.ID[id].Key) + weight
				m[cm.ID[id].Key] = item
			}
		}
		results <- result[Data]{m: m}
	}
}

type job struct {
	substring string
}

type result[Data any] struct {
	m map[string]workerResult[Data]
}

func (cm *ClosestMatch[Data]) match(searchWord string) map[string]workerResult[Data] {
	searchSubstrings := cm.splitWord(strings.ToLower(searchWord))
	searchSubstringsLen := len(searchSubstrings)

	jobs := make(chan job, searchSubstringsLen)
	results := make(chan result[Data], searchSubstringsLen)
	workers := 8

	for w := 1; w <= workers; w++ {
		go cm.worker(w, jobs, results)
	}

	for substring := range searchSubstrings {
		jobs <- job{substring: substring}
	}
	close(jobs)

	m := make(map[string]workerResult[Data])
	for a := 1; a <= searchSubstringsLen; a++ {
		r := <-results
		for key := range r.m {
			if _, ok := m[key]; ok {
				x := m[key]
				x.Value += r.m[key].Value
				m[key] = x
			} else {
				m[key] = r.m[key]
			}
		}
	}

	return m
}

// Closest searches for the `searchWord` and returns the closest match
func (cm *ClosestMatch[Data]) Closest(searchWord string) string {
	for _, pair := range rankByWordCount[Data](cm.match(searchWord)) {
		return pair.Key
	}
	return ""
}

// ClosestN searches for the `searchWord` and returns the n closests matches
func (cm *ClosestMatch[Data]) ClosestN(searchWord string, max int) []Match[Data] {
	matched := rankByWordCount[Data](cm.match(searchWord))
	if len(matched) < max {
		max = len(matched)
	}
	return matched[:max]
}

func rankByWordCount[Data any](wordFrequencies map[string]workerResult[Data]) MatchList[Data] {
	pl := make(MatchList[Data], len(wordFrequencies))
	i := 0
	for k, v := range wordFrequencies {
		pl[i] = Match[Data]{
			Key:   k,
			Data:  v.Data,
			Value: v.Value,
		}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}

// Match is a struct that contains the matched string, the data associated with
// the matched string and the score.
type Match[Data any] struct {
	Key   string // matched string
	Data  Data   // data associated with the matched string
	Value int    // score
}

// MatchList is a list of Match structs. It implements the sort interface.
type MatchList[Data any] []Match[Data]

func (p MatchList[Data]) Len() int           { return len(p) }
func (p MatchList[Data]) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p MatchList[Data]) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (cm *ClosestMatch[Data]) splitWord(word string) map[string]struct{} {
	wordHash := make(map[string]struct{})
	for _, j := range cm.SubstringSizes {
		for i := 0; i < len([]rune(word))-j+1; i++ {
			substring := string([]rune(word)[i : i+j])
			if len(strings.TrimSpace(substring)) > 0 {
				wordHash[string([]rune(word)[i:i+j])] = struct{}{}
			}
		}
	}
	if len(wordHash) == 0 {
		wordHash[word] = struct{}{}
	}
	return wordHash
}

// AccuracyMutatingWords runs some basic tests against the wordlist to
// see how accurate this bag-of-characters method is against
// the target dataset
func (cm *ClosestMatch[Data]) AccuracyMutatingWords() float64 {
	rand.Seed(1)
	percentCorrect := 0.0
	numTrials := 0.0

	for wordTrials := 0; wordTrials < 200; wordTrials++ {

		var testString, originalTestString string
		testStringNum := rand.Intn(len(cm.ID))
		i := 0
		for id := range cm.ID {
			i++
			if i != testStringNum {
				continue
			}
			originalTestString = cm.ID[id].Key
			break
		}

		var words []string
		choice := rand.Intn(3)
		if choice == 0 {
			// remove a random word
			words = strings.Split(originalTestString, " ")
			if len(words) < 3 {
				continue
			}
			deleteWordI := rand.Intn(len(words))
			words = append(words[:deleteWordI], words[deleteWordI+1:]...)
			testString = strings.Join(words, " ")
		} else if choice == 1 {
			// remove a random word and reverse
			words = strings.Split(originalTestString, " ")
			if len(words) > 1 {
				deleteWordI := rand.Intn(len(words))
				words = append(words[:deleteWordI], words[deleteWordI+1:]...)
				for left, right := 0, len(words)-1; left < right; left, right = left+1, right-1 {
					words[left], words[right] = words[right], words[left]
				}
			} else {
				continue
			}
			testString = strings.Join(words, " ")
		} else {
			// remove a random word and shuffle and replace 2 random letters
			words = strings.Split(originalTestString, " ")
			if len(words) > 1 {
				deleteWordI := rand.Intn(len(words))
				words = append(words[:deleteWordI], words[deleteWordI+1:]...)
				for i := range words {
					j := rand.Intn(i + 1)
					words[i], words[j] = words[j], words[i]
				}
			}
			testString = strings.Join(words, " ")
			if len(testString) == 0 {
				continue
			}
			ii := rand.Intn(len(testString))
			testString = testString[:ii] + string(letters[rand.Intn(len(letters))]) + testString[ii+1:]
			ii = rand.Intn(len(testString))
			testString = testString[:ii] + string(letters[rand.Intn(len(letters))]) + testString[ii+1:]
		}
		closest := cm.Closest(testString)
		if closest == originalTestString {
			percentCorrect += 1.0
		} else {
			//fmt.Printf("Original: %s, Mutilated: %s, Match: %s\n", originalTestString, testString, closest)
		}
		numTrials += 1.0
	}
	return 100.0 * percentCorrect / numTrials
}

// AccuracyMutatingLetters runs some basic tests against the wordlist to
// see how accurate this bag-of-characters method is against
// the target dataset when mutating individual letters (adding, removing, changing)
func (cm *ClosestMatch[Data]) AccuracyMutatingLetters() float64 {
	rand.Seed(1)
	percentCorrect := 0.0
	numTrials := 0.0

	for wordTrials := 0; wordTrials < 200; wordTrials++ {

		var testString, originalTestString string
		testStringNum := rand.Intn(len(cm.ID))
		i := 0
		for id := range cm.ID {
			i++
			if i != testStringNum {
				continue
			}
			originalTestString = cm.ID[id].Key
			break
		}
		testString = originalTestString

		choice := rand.Intn(3)
		if choice == 0 {
			// replace random letter
			ii := rand.Intn(len(testString))
			testString = testString[:ii] + string(letters[rand.Intn(len(letters))]) + testString[ii+1:]
		} else if choice == 1 {
			// delete random letter
			ii := rand.Intn(len(testString))
			testString = testString[:ii] + testString[ii+1:]
		} else {
			// add random letter
			ii := rand.Intn(len(testString))
			testString = testString[:ii] + string(letters[rand.Intn(len(letters))]) + testString[ii:]
		}
		closest := cm.Closest(testString)
		if closest == originalTestString {
			percentCorrect += 1.0
		} else {
			//fmt.Printf("Original: %s, Mutilated: %s, Match: %s\n", originalTestString, testString, closest)
		}
		numTrials += 1.0
	}

	return 100.0 * percentCorrect / numTrials
}
