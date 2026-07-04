// Package search turns a user query string into a Bluge query, executes it
// against the cached index snapshot and hydrates the ranked hits from the KV
// store. The whole path is allocation-light and reader-cached so a query
// resolves in well under 100 ms even at millions of documents.
package search

import (
	"context"
	"strings"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"

	"github.com/Maringouin10/stackia-search/internal/index"
	"github.com/Maringouin10/stackia-search/internal/model"
	"github.com/Maringouin10/stackia-search/internal/store"
)

const (
	maxSize     = 50 // hard cap on returned hits — protects tail latency
	defaultSize = 10
	// Field boosts: a title hit is worth far more than a body hit.
	titleBoost = 2.5
	bodyBoost  = 1.0
)

type Searcher struct {
	ix *index.Index
	kv *store.Store
}

func New(ix *index.Index, kv *store.Store) *Searcher {
	return &Searcher{ix: ix, kv: kv}
}

// Query parses `q`, runs it, and returns hydrated results. It never returns a
// nil response for a well-formed (even if empty) query.
func (s *Searcher) Query(ctx context.Context, q string, size int) (*model.SearchResponse, error) {
	start := time.Now()
	if size <= 0 {
		size = defaultSize
	}
	if size > maxSize {
		size = maxSize
	}

	resp := &model.SearchResponse{Query: q, Results: []model.SearchResult{}}

	site, terms := splitSiteFilter(q)
	if strings.TrimSpace(terms) == "" && site == "" {
		resp.TookMS = msSince(start)
		return resp, nil
	}

	query := s.buildQuery(terms, site)
	req := bluge.NewTopNSearch(size, query).WithStandardAggregations()

	// A short deadline guards against a pathological query blowing the budget.
	sctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	total, err := s.ix.SearchTopN(sctx, req, func(hit *search.DocumentMatch) error {
		var url string
		if err := hit.VisitStoredFields(func(field string, value []byte) bool {
			if field == index.FieldID {
				url = string(value)
				return false // stop; _id is all we stored
			}
			return true
		}); err != nil {
			return err
		}
		if url == "" {
			return nil
		}
		res := model.SearchResult{URL: url, Score: hit.Score}
		// Hydrate title/snippet/host from the KV store; tolerate a missing row.
		if m, err := s.kv.GetMeta(url); err == nil {
			res.Title = m.Title
			res.Snippet = m.Snippet
			res.Host = m.Host
		}
		if res.Title == "" {
			res.Title = url
		}
		if hit.Score > resp.MaxScore {
			resp.MaxScore = hit.Score
		}
		resp.Results = append(resp.Results, res)
		return nil
	})
	if err != nil {
		return nil, err
	}

	resp.Total = total
	resp.TookMS = msSince(start)
	return resp, nil
}

// buildQuery assembles the ranking query:
//   - a quoted query becomes a phrase match (order-sensitive);
//   - otherwise it is a title-boosted OR body match;
//   - an optional site:<host> clause is ANDed as an exact filter.
//
// Term analysis (lowercasing, stop-word removal, stemming) is applied by Bluge
// using the analyzer we attach, matching how the corpus was indexed.
func (s *Searcher) buildQuery(terms, site string) bluge.Query {
	ana := s.ix.Analyzer()
	terms = strings.TrimSpace(terms)

	var textQuery bluge.Query
	if isQuoted(terms) {
		phrase := strings.Trim(terms, `"`)
		tp := bluge.NewMatchPhraseQuery(phrase).SetField(index.FieldTitle).SetAnalyzer(ana).SetBoost(titleBoost)
		bp := bluge.NewMatchPhraseQuery(phrase).SetField(index.FieldBody).SetAnalyzer(ana).SetBoost(bodyBoost)
		textQuery = bluge.NewBooleanQuery().AddShould(tp, bp).SetMinShould(1)
	} else {
		tq := bluge.NewMatchQuery(terms).SetField(index.FieldTitle).
			SetAnalyzer(ana).SetOperator(bluge.MatchQueryOperatorOr).SetBoost(titleBoost)
		bq := bluge.NewMatchQuery(terms).SetField(index.FieldBody).
			SetAnalyzer(ana).SetOperator(bluge.MatchQueryOperatorOr).SetBoost(bodyBoost)
		textQuery = bluge.NewBooleanQuery().AddShould(tq, bq).SetMinShould(1)
	}

	if site == "" {
		return textQuery
	}
	// site:<host> is an exact keyword filter ANDed with the text query.
	hostFilter := bluge.NewTermQuery(strings.ToLower(site)).SetField(index.FieldHost)
	if strings.TrimSpace(terms) == "" {
		return hostFilter
	}
	return bluge.NewBooleanQuery().AddMust(textQuery, hostFilter)
}

// splitSiteFilter extracts a single `site:host` token from the query, returning
// the host and the remaining query text.
func splitSiteFilter(q string) (site, rest string) {
	fields := strings.Fields(q)
	kept := fields[:0]
	for _, f := range fields {
		if strings.HasPrefix(strings.ToLower(f), "site:") && len(f) > 5 {
			site = f[5:]
			continue
		}
		kept = append(kept, f)
	}
	return site, strings.Join(kept, " ")
}

func isQuoted(s string) bool {
	return len(s) >= 2 && strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)
}

func msSince(t time.Time) float64 {
	return float64(time.Since(t).Microseconds()) / 1000.0
}
