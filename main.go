package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"regexp"
	"strings"
	"sync"
	"time"
)

const defaultAPI = "https://json.vnres.co"

// userAgent is set from -user-agent at startup; getJSONP reads it.
var userAgent = "namhau-iptv-tool/0.1"

type Anchor struct {
	NickName string `json:"nickName"`
}

type LiveRoom struct {
	RoomNum      string `json:"roomNum"`
	Title        string `json:"title"`
	LiveStatus   int    `json:"liveStatus"`
	LiveType     int    `json:"liveType"`
	LiveTypeName string `json:"liveTypeName,omitempty"`
	Anchor       Anchor `json:"anchor"`
	Cover        string `json:"cover"`
}

type Stream struct {
	FLV    string `json:"flv"`
	HDFlv  string `json:"hdFlv"`
	M3U8   string `json:"m3u8"`
	HDM3U8 string `json:"hdM3u8"`
}

type roomsPayload struct {
	Code int                        `json:"code"`
	Msg  string                     `json:"msg"`
	Data map[string]json.RawMessage `json:"data"`
}

type detailPayload struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Room   LiveRoom `json:"room"`
		Stream Stream   `json:"stream"`
	} `json:"data"`
}

type Channel struct {
	RoomNum string `json:"room_num"`
	Title   string `json:"title"`
	Anchor  string `json:"anchor,omitempty"`
	Logo    string `json:"logo,omitempty"`
	Group   string `json:"group,omitempty"`
	URL     string `json:"url"`
	Format  string `json:"format"`
}

type ScanResult struct {
	FetchedAt       time.Time `json:"fetched_at"`
	Groups          int       `json:"groups"`
	RoomsReported   int       `json:"rooms_reported"`
	UniqueLiveRooms int       `json:"unique_live_rooms"`
	WithStream      int       `json:"with_stream"`
	Channels        []Channel `json:"channels"`
	Errors          []string  `json:"errors,omitempty"`
}

type Config struct {
	APIBase  string
	Out      string
	JSONOut  string
	Format   string
	Interval time.Duration
	Workers  int
	Timeout  time.Duration
	Once     bool

	Publish      bool
	GitHubToken  string
	GitHubRepo   string
	GitHubBranch string
	GitHubPath   string
	EPG          string
	Listen       string
	ForcePublish time.Duration
	UserAgent    string
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.APIBase, "api", defaultAPI, "API base URL")
	flag.StringVar(&cfg.Out, "out", "playlist.m3u", "output path; use - for stdout")
	flag.StringVar(&cfg.Format, "format", "m3u", "output format: m3u or json")
	flag.StringVar(&cfg.JSONOut, "json", "", "optional JSON scan report path")
	flag.DurationVar(&cfg.Interval, "interval", 0, "refresh interval, e.g. 60s; zero runs one scan")
	flag.IntVar(&cfg.Workers, "workers", 8, "maximum concurrent room-detail requests")
	flag.DurationVar(&cfg.Timeout, "timeout", 15*time.Second, "HTTP timeout per request")
	flag.BoolVar(&cfg.Once, "once", false, "run exactly one scan (overrides -interval)")
	flag.BoolVar(&cfg.Publish, "publish", false, "commit the IPTV JSON channels to a GitHub repository after each scan")
	flag.StringVar(&cfg.GitHubToken, "github-token", os.Getenv("GITHUB_TOKEN"), "GitHub token; defaults to $GITHUB_TOKEN")
	flag.StringVar(&cfg.GitHubRepo, "github-repo", "htuananh1/userscript", "GitHub repository as owner/name")
	flag.StringVar(&cfg.GitHubBranch, "github-branch", "main", "branch to commit to")
	flag.StringVar(&cfg.GitHubPath, "github-path", "Socolive.json", "file path inside the repository")
	flag.StringVar(&cfg.EPG, "epg", "", "EPG XMLTV URL(s) for the M3U url-tvg attribute; comma-separated")
	flag.StringVar(&cfg.Listen, "listen", "", "serve playlist and /play/{room} redirects on host:port")
	flag.DurationVar(&cfg.ForcePublish, "force-publish", 4*time.Hour, "republish after this idle duration to keep stream tokens fresh; 0 disables")
	flag.StringVar(&cfg.UserAgent, "user-agent", "namhau-iptv-tool/0.1", "User-Agent header for API requests")
	flag.Parse()

	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	userAgent = cfg.UserAgent
	cfg.APIBase = strings.TrimRight(cfg.APIBase, "/")
	cfg.Format = strings.ToLower(strings.TrimSpace(cfg.Format))
	if cfg.Format != "m3u" && cfg.Format != "json" {
		log.Fatalf("unsupported -format %q; use m3u or json", cfg.Format)
	}
	if cfg.Publish {
		cfg.GitHubToken = strings.TrimSpace(cfg.GitHubToken)
		if cfg.GitHubToken == "" {
			log.Fatal("-publish needs a GitHub token; pass -github-token or set $GITHUB_TOKEN")
		}
		if cfg.GitHubRepo == "" || cfg.GitHubPath == "" {
			log.Fatal("-publish needs -github-repo (owner/name) and -github-path")
		}
		cfg.GitHubRepo = strings.Trim(cfg.GitHubRepo, "/")
		cfg.GitHubPath = strings.Trim(cfg.GitHubPath, "/")
	}

	client := &http.Client{Timeout: cfg.Timeout}
	store := NewChannelStore()
	if cfg.Listen != "" {
		server := &http.Server{
			Addr:              cfg.Listen,
			Handler:           serverHandler(client, cfg, store),
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			log.Printf("http server listening on %s (playlist: any *.m3u path, streams: /play/{room})", cfg.Listen)
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("http server: %v", err)
			}
		}()
	}
	scan := func() error {
		result, err := scanOnce(context.Background(), client, cfg)
		if err != nil {
			return err
		}
		var writeErr error
		if cfg.Format == "json" {
			writeErr = writeIPTVJSON(cfg.Out, result.Channels)
		} else {
			writeErr = writePlaylist(cfg.Out, result.Channels, cfg.EPG)
		}
		if writeErr != nil {
			return fmt.Errorf("write playlist: %w", writeErr)
		}
		if cfg.JSONOut != "" {
			if err := writeJSON(cfg.JSONOut, result); err != nil {
				return fmt.Errorf("write report: %w", err)
			}
		}
		store.Replace(result.Channels)
		if cfg.Publish {
			newItems := iptvJSONItems(result.Channels)
			fpNew := fingerprintItems(newItems)
			st, found := fetchRemoteState(context.Background(), client, cfg)

			if len(newItems) == 0 && found {
				return errors.New("scan returned no channels; keeping previous playlist")
			}

			changed := !found || fpNew != st.Fingerprint
			expired := cfg.ForcePublish > 0 &&
				(!found || st.PublishedAt.IsZero() || time.Since(st.PublishedAt) >= cfg.ForcePublish)
			if !changed && !expired {
				log.Printf("no changes since last publish (%s ago); skip GitHub commit",
					time.Since(st.PublishedAt).Round(time.Second))
				return nil
			}
			reason := "content changed"
			if !changed {
				reason = fmt.Sprintf("token refresh after %s", time.Since(st.PublishedAt).Round(time.Second))
			}

			jsonData, err := iptvJSONBytes(result.Channels)
			if err != nil {
				return fmt.Errorf("encode IPTV JSON: %w", err)
			}
			jsonURL, err := publishToGitHub(context.Background(), client, cfg, cfg.GitHubPath, jsonData, fpNew)
			if err != nil {
				return fmt.Errorf("publish %s: %w", cfg.GitHubPath, err)
			}
			log.Printf("published to GitHub (%s): %s", reason, jsonURL)

			m3uPath := m3uSiblingPath(cfg.GitHubPath)
			m3uData, err := m3uBytes(result.Channels, cfg.EPG, time.Now())
			if err != nil {
				return fmt.Errorf("encode M3U: %w", err)
			}
			m3uURL, err := publishToGitHub(context.Background(), client, cfg, m3uPath, m3uData, fpNew)
			if err != nil {
				return fmt.Errorf("publish %s: %w", m3uPath, err)
			}
			log.Printf("published to GitHub (%s): %s", reason, m3uURL)
		}
		log.Printf("live reported=%d unique=%d streams=%d playlist=%s", result.RoomsReported, result.UniqueLiveRooms, result.WithStream, cfg.Out)
		for _, e := range result.Errors {
			log.Printf("warning: %s", e)
		}
		return nil
	}

	if cfg.Once || cfg.Interval <= 0 {
		if err := scan(); err != nil {
			log.Fatal(err)
		}
		if cfg.Listen == "" {
			return
		}
		// Keep serving HTTP requests even in single-scan mode.
		log.Printf("scan done; serving http on %s until interrupted", cfg.Listen)
		select {}
	}

	if err := scan(); err != nil {
		log.Printf("scan failed: %v", err)
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := scan(); err != nil {
			log.Printf("scan failed: %v", err)
		}
	}
}

func scanOnce(ctx context.Context, client *http.Client, cfg Config) (ScanResult, error) {
	result := ScanResult{FetchedAt: time.Now().UTC()}
	roomsURL := apiURL(cfg.APIBase, "/all_live_rooms.json", "all_live_rooms")
	var payload roomsPayload
	if err := getJSONP(ctx, client, roomsURL, &payload); err != nil {
		return result, fmt.Errorf("fetch live rooms: %w", err)
	}
	if payload.Code != 200 {
		return result, fmt.Errorf("live rooms API returned code %d: %s", payload.Code, payload.Msg)
	}

	unique := make(map[string]LiveRoom)
	for _, raw := range payload.Data {
		var rooms []LiveRoom
		if err := json.Unmarshal(raw, &rooms); err != nil {
			// all_live_rooms also contains non-room fields such as scroll/hot.
			continue
		}
		result.Groups++
		result.RoomsReported += len(rooms)
		for _, room := range rooms {
			if room.RoomNum == "" || room.LiveStatus != 1 {
				continue
			}
			// The API can report one room in more than one group. Deduplicate
			// by room number so the playlist does not contain duplicates.
			if _, exists := unique[room.RoomNum]; !exists {
				unique[room.RoomNum] = room
			}
		}
	}
	result.UniqueLiveRooms = len(unique)

	rooms := make([]LiveRoom, 0, len(unique))
	for _, room := range unique {
		rooms = append(rooms, room)
	}
	sort.Slice(rooms, func(i, j int) bool { return rooms[i].RoomNum < rooms[j].RoomNum })

	type detailResult struct {
		channel Channel
		err     error
	}
	jobs := make(chan LiveRoom)
	results := make(chan detailResult, len(rooms))
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for room := range jobs {
				channel, err := fetchChannel(ctx, client, cfg.APIBase, room)
				results <- detailResult{channel: channel, err: err}
			}
		}()
	}
	go func() {
		for _, room := range rooms {
			jobs <- room
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for item := range results {
		if item.err != nil {
			result.Errors = append(result.Errors, item.err.Error())
			continue
		}
		result.Channels = append(result.Channels, item.channel)
	}
	sort.Slice(result.Channels, func(i, j int) bool { return result.Channels[i].RoomNum < result.Channels[j].RoomNum })
	result.WithStream = len(result.Channels)
	return result, nil
}

func fetchChannel(ctx context.Context, client *http.Client, apiBase string, room LiveRoom) (Channel, error) {
	endpoint := apiURL(apiBase, "/room/"+url.PathEscape(room.RoomNum)+"/detail.json", "detail")
	var payload detailPayload
	if err := getJSONP(ctx, client, endpoint, &payload); err != nil {
		return Channel{}, fmt.Errorf("room %s: %w", room.RoomNum, err)
	}
	if payload.Code != 200 {
		return Channel{}, fmt.Errorf("room %s: API returned code %d: %s", room.RoomNum, payload.Code, payload.Msg)
	}
	stream := payload.Data.Stream
	streamURL, format := stream.HDM3U8, "hls"
	if streamURL == "" {
		streamURL = stream.M3U8
	}
	if streamURL == "" {
		streamURL, format = stream.HDFlv, "flv"
		if streamURL == "" {
			streamURL = stream.FLV
		}
	}
	if streamURL == "" {
		return Channel{}, fmt.Errorf("room %s: no stream URL", room.RoomNum)
	}
	name := room.Title
	if name == "" {
		name = payload.Data.Room.Title
	}
	anchor := room.Anchor.NickName
	if anchor == "" {
		anchor = payload.Data.Room.Anchor.NickName
	}
	logo := room.Cover
	if logo == "" {
		logo = payload.Data.Room.Cover
	}
	group := room.LiveTypeName
	if group == "" {
		group = payload.Data.Room.LiveTypeName
	}
	if group == "" {
		group = "SocoLive"
	}
	return Channel{RoomNum: room.RoomNum, Title: name, Anchor: anchor, Logo: logo, Group: group, URL: streamURL, Format: format}, nil
}

func apiURL(base, path, callback string) string {
	now := time.Now().Unix()
	v := url.Values{}
	v.Set("callback", callback)
	v.Set("v", fmt.Sprint(now))
	v.Set("_", fmt.Sprint(now))
	return strings.TrimRight(base, "/") + path + "?" + v.Encode()
}

func getJSONP(ctx context.Context, client *http.Client, endpoint string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	jsonBody, err := unwrapJSONP(body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(jsonBody, out); err != nil {
		return fmt.Errorf("decode JSONP: %w", err)
	}
	return nil
}

func unwrapJSONP(body []byte) ([]byte, error) {
	text := strings.TrimSpace(string(body))
	start := strings.IndexAny(text, "{[")
	end := strings.LastIndexAny(text, "}]")
	if start < 0 || end < start {
		return nil, errors.New("response is not JSONP/JSON")
	}
	return []byte(text[start : end+1]), nil
}

// m3uBytes renders the playlist in the extended M3U style that IPTV apps
// such as OTT Navigator and TiviMate expect: an url-tvg EPG header, an
// info block, and per-channel tvg-id/tvg-logo/group-logo/group-title.
func m3uBytes(channels []Channel, epg string, now time.Time) ([]byte, error) {
	var b strings.Builder
	b.WriteString("#EXTM3U")
	if epg = strings.TrimSpace(epg); epg != "" {
		b.WriteString(" url-tvg=\"" + escapeM3U(epg) + "\"")
	}
	b.WriteString("\n")
	b.WriteString("# ======================================\n")
	b.WriteString("# SOCOLIVE LIVE PLAYLIST\n")
	b.WriteString("# ======================================\n")
	b.WriteString("# Status       : ONLINE\n")
	fmt.Fprintf(&b, "# Channels     : %d\n", len(channels))
	fmt.Fprintf(&b, "# Updated      : %s\n", now.Format("02/01/2006 15:04:05"))
	b.WriteString("# Player       : OTT Navigator / TiviMate\n")
	b.WriteString("# ======================================\n\n")
	for _, channel := range channels {
		name := channel.Title
		if name == "" {
			name = "Room " + channel.RoomNum
		}
		if channel.Anchor != "" {
			name += " - " + channel.Anchor
		}
		group := channel.Group
		if group == "" {
			group = "SocoLive"
		}
		fmt.Fprintf(&b, "#EXTINF:-1 tvg-id=\"%s\" tvg-logo=\"%s\" group-logo=\"%s\" group-title=\"%s\",%s\n",
			escapeM3U("room-"+channel.RoomNum),
			escapeM3U(channel.Logo),
			escapeM3U(channel.Logo),
			escapeM3U(group),
			escapeM3U(name))
		b.WriteString(channel.URL)
		b.WriteByte('\n')
	}
	return []byte(b.String()), nil
}

func writePlaylist(path string, channels []Channel, epg string) error {
	data, err := m3uBytes(channels, epg, time.Now())
	if err != nil {
		return err
	}
	if path == "-" {
		_, err := os.Stdout.Write(data)
		return err
	}
	return atomicWrite(path, data)
}

func escapeM3U(s string) string {
	return strings.NewReplacer("\"", "'", "\r", " ", "\n", " ").Replace(s)
}

type IPTVJSONChannel struct {
	ID     string `json:"id"`
	TVGID  string `json:"tvg_id"`
	Name   string `json:"name"`
	Logo   string `json:"logo,omitempty"`
	Group  string `json:"group"`
	URL    string `json:"url"`
	Format string `json:"type,omitempty"`
}

// iptvJSONItems converts channels to the wire format used by both the JSON
// output and the change detector.
func iptvJSONItems(channels []Channel) []IPTVJSONChannel {
	items := make([]IPTVJSONChannel, 0, len(channels))
	for _, channel := range channels {
		name := channel.Title
		if channel.Anchor != "" {
			name += " - " + channel.Anchor
		}
		group := channel.Group
		if group == "" {
			group = "SocoLive"
		}
		items = append(items, IPTVJSONChannel{
			ID: channel.RoomNum, TVGID: "room-" + channel.RoomNum,
			Name: name, Logo: channel.Logo, Group: group,
			URL: channel.URL, Format: channel.Format,
		})
	}
	return items
}

func iptvJSONBytes(channels []Channel) ([]byte, error) {
	data, err := json.MarshalIndent(iptvJSONItems(channels), "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func writeIPTVJSON(path string, channels []Channel) error {
	data, err := iptvJSONBytes(channels)
	if err != nil {
		return err
	}
	if path == "-" {
		_, err := os.Stdout.Write(data)
		return err
	}
	return atomicWrite(path, data)
}

func writeJSON(path string, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if path == "-" {
		_, err := os.Stdout.Write(data)
		return err
	}
	return atomicWrite(path, data)
}

// fingerprint hashes the published JSON representation of the channels,
// ignoring volatile URL tokens (query strings), so a re-scan of unchanged
// rooms produces the same value while any real change (room list, name,
// logo, group, stream host/path, format) produces a new one. It works on
// IPTVJSONChannel so the same function can hash both a fresh scan and the
// file currently published on GitHub.
func fingerprintItems(items []IPTVJSONChannel) string {
	h := sha256.New()
	for _, it := range items {
		base := it.URL
		if u, err := url.Parse(it.URL); err == nil {
			u.RawQuery = ""
			u.Fragment = ""
			base = u.String()
		}
		fmt.Fprintf(h, "%s|%s|%s|%s|%s|%s|%s\n",
			it.ID, it.TVGID, it.Name, it.Logo, it.Group, it.Format, base)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// publishState tracks when and what was last committed. The fingerprint and
// timestamp are embedded in the commit message itself so the state survives
// across machines — including ephemeral GitHub Actions runners.
type publishState struct {
	Fingerprint string    `json:"fingerprint"`
	PublishedAt time.Time `json:"published_at"`
}

// shortHash keeps commit messages readable; collisions are astronomically
// unlikely for change detection and a mismatch only causes one extra commit.
func shortHash(fp string) string {
	if len(fp) > 16 {
		return fp[:16]
	}
	return fp
}

const fpMarker = "[fp="
const atMarker = "[at="

// parseStateFromCommit extracts the fingerprint and timestamp embedded in a
// publish commit message such as:
//
//	update Socolive.json [fp=<sha256>] [at=2026-08-24T13:36:05Z]
func parseStateFromCommit(message string) (publishState, bool) {
	var st publishState
	i := strings.Index(message, fpMarker)
	if i < 0 {
		return st, false
	}
	rest := message[i+len(fpMarker):]
	j := strings.Index(rest, "]")
	if j <= 0 {
		return st, false
	}
	st.Fingerprint = rest[:j]
	k := strings.Index(message, atMarker)
	if k >= 0 {
		rest := message[k+len(atMarker):]
		if l := strings.Index(rest, "]"); l > 0 {
			if t, err := time.Parse(time.RFC3339, rest[:l]); err == nil {
				st.PublishedAt = t.UTC()
			}
		}
	}
	return st, true
}

// fetchRemoteState reads the last publish fingerprint and timestamp from the
// most recent commit touching the JSON file. This makes change detection work
// from any environment — including fresh GitHub Actions runners with no disk.
func fetchRemoteState(ctx context.Context, client *http.Client, cfg Config) (publishState, bool) {
	api := fmt.Sprintf("https://api.github.com/repos/%s/commits?path=%s&per_page=1",
		cfg.GitHubRepo, url.QueryEscape(cfg.GitHubPath))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, api, nil)
	if err != nil {
		return publishState{}, false
	}
	setGitHubHeaders(req.Header, cfg.GitHubToken)
	resp, err := client.Do(req)
	if err != nil {
		return publishState{}, false
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return publishState{}, false
	}
	var commits []struct {
		Commit struct {
			Message string `json:"message"`
		} `json:"commit"`
	}
	if json.Unmarshal(body, &commits) != nil || len(commits) == 0 {
		return publishState{}, false
	}
	return parseStateFromCommit(commits[0].Commit.Message)
}

type githubContent struct {
	SHA     string `json:"sha"`
	HTMLURL string `json:"html_url"`
}

// githubPutResponse is the reply to a Contents API create/update request,
// where "content" describes the stored file and "commit" the created commit.
type githubPutResponse struct {
	Content githubContent `json:"content"`
	Commit  githubContent `json:"commit"`
}

// githubContentsURL builds the Contents API endpoint for a file path,
// escaping each path segment but keeping "/" separators intact.
func githubContentsURL(repo, path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return fmt.Sprintf("https://api.github.com/repos/%s/contents/%s", repo, strings.Join(parts, "/"))
}

func setGitHubHeaders(header http.Header, token string) {
	header.Set("Accept", "application/vnd.github+json")
	header.Set("Authorization", "Bearer "+token)
	header.Set("X-GitHub-Api-Version", "2022-11-28")
	header.Set("User-Agent", "namhau-iptv-tool/0.1")
}

func githubErrorMessage(body []byte) string {
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Message == "" {
		text := strings.TrimSpace(string(body))
		if text == "" {
			text = "(empty response body)"
		}
		if len(text) > 200 {
			text = text[:200]
		}
		return text
	}
	return payload.Message
}

// m3uSiblingPath returns the M3U path published next to the JSON file,
// e.g. "Socolive.json" -> "Socolive.m3u".
func m3uSiblingPath(path string) string {
	if ext := filepath.Ext(path); ext != "" {
		return strings.TrimSuffix(path, ext) + ".m3u"
	}
	return path + ".m3u"
}

// publishToGitHub commits data to repo:path on the configured branch via the
// Contents API. Updating an existing file requires its current blob SHA, so
// the file is fetched first; a 404 simply means the file will be created.
// fp is embedded in the commit message so future runs — including ephemeral
// GitHub Actions runners — can detect what was last published.
func publishToGitHub(ctx context.Context, client *http.Client, cfg Config, path string, data []byte, fp string) (string, error) {
	endpoint := githubContentsURL(cfg.GitHubRepo, path)

	var sha string
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?ref="+url.QueryEscape(cfg.GitHubBranch), nil)
	if err != nil {
		return "", err
	}
	setGitHubHeaders(req.Header, cfg.GitHubToken)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if readErr != nil {
		return "", readErr
	}
	switch resp.StatusCode {
	case http.StatusOK:
		var existing githubContent
		if err := json.Unmarshal(body, &existing); err != nil {
			return "", fmt.Errorf("decode existing file: %w", err)
		}
		sha = existing.SHA
	case http.StatusNotFound:
		// File does not exist yet; create it without a SHA.
	default:
		return "", fmt.Errorf("lookup %s: HTTP %d: %s", path, resp.StatusCode, githubErrorMessage(body))
	}

	payload := struct {
		Message string `json:"message"`
		Content string `json:"content"`
		Branch  string `json:"branch"`
		SHA     string `json:"sha,omitempty"`
	}{
		Message: fmt.Sprintf("update %s [fp=%s] [at=%s]",
			filepath.Base(path), shortHash(fp), time.Now().UTC().Format(time.RFC3339)),
		Content: base64.StdEncoding.EncodeToString(data),
		Branch:  cfg.GitHubBranch,
		SHA:     sha,
	}
	putBody, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(putBody))
	if err != nil {
		return "", err
	}
	setGitHubHeaders(req.Header, cfg.GitHubToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		return "", err
	}
	body, readErr = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if readErr != nil {
		return "", readErr
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("commit %s: HTTP %d: %s", path, resp.StatusCode, githubErrorMessage(body))
	}
	var committed githubPutResponse
	if err := json.Unmarshal(body, &committed); err != nil {
		return "", fmt.Errorf("decode commit response: %w", err)
	}
	switch {
	case committed.Commit.HTMLURL != "":
		return committed.Commit.HTMLURL, nil
	case committed.Content.HTMLURL != "":
		return committed.Content.HTMLURL, nil
	default:
		return endpoint, nil
	}
}

// storeEntry keeps a channel plus when its stream URL was fetched, so the
// redirect handler can refresh stale tokens on demand.
type storeEntry struct {
	channel   Channel
	fetchedAt time.Time
}

// ChannelStore holds the latest scan result for the HTTP server.
type ChannelStore struct {
	mu    sync.RWMutex
	order []string
	items map[string]storeEntry
}

func NewChannelStore() *ChannelStore {
	return &ChannelStore{items: make(map[string]storeEntry)}
}

// Replace swaps in a fresh scan result (channels must already be sorted).
func (s *ChannelStore) Replace(channels []Channel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.order = s.order[:0]
	for _, ch := range channels {
		s.items[ch.RoomNum] = storeEntry{channel: ch, fetchedAt: now}
		s.order = append(s.order, ch.RoomNum)
	}
}

func (s *ChannelStore) Get(room string) (storeEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.items[room]
	return entry, ok
}

func (s *ChannelStore) Put(ch Channel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.items[ch.RoomNum]; !exists {
		s.order = append(s.order, ch.RoomNum)
	}
	s.items[ch.RoomNum] = storeEntry{channel: ch, fetchedAt: time.Now()}
}

func (s *ChannelStore) List() []Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Channel, 0, len(s.order))
	for _, room := range s.order {
		out = append(out, s.items[room].channel)
	}
	return out
}

// baseURL reconstructs the external URL prefix, honouring reverse proxies.
func baseURL(r *http.Request) string {
	scheme := "http"
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	} else if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

var roomNumPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func serverHandler(client *http.Client, cfg Config, store *ChannelStore) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, ".m3u"):
			handlePlaylist(w, r, cfg, store)
		case strings.HasPrefix(r.URL.Path, "/play/"):
			handlePlay(w, r, client, cfg, store)
		default:
			http.NotFound(w, r)
		}
	})
	return mux
}

// handlePlaylist renders the M3U with short /play/{room} links instead of
// raw CDN URLs; apps follow the redirects like they do for easport-style
// playlists.
func handlePlaylist(w http.ResponseWriter, r *http.Request, cfg Config, store *ChannelStore) {
	base := baseURL(r)
	channels := store.List()
	for i := range channels {
		channels[i].URL = base + "/play/" + url.PathEscape(channels[i].RoomNum)
	}
	data, err := m3uBytes(channels, cfg.EPG, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(data)
}

// handlePlay resolves a room to its current stream URL and redirects. URLs
// are refreshed when older than refreshAfter so tokens never expire mid-use.
func handlePlay(w http.ResponseWriter, r *http.Request, client *http.Client, cfg Config, store *ChannelStore) {
	const refreshAfter = 10 * time.Minute
	room := strings.TrimPrefix(r.URL.Path, "/play/")
	if !roomNumPattern.MatchString(room) || room == "" {
		http.Error(w, "invalid room", http.StatusBadRequest)
		return
	}
	entry, ok := store.Get(room)
	if !ok || time.Since(entry.fetchedAt) > refreshAfter {
		fresh, err := fetchChannel(r.Context(), client, cfg.APIBase, LiveRoom{RoomNum: room})
		if err != nil {
			if !ok {
				log.Printf("play %s: %v", room, err)
				http.Error(w, "channel unavailable: "+err.Error(), http.StatusBadGateway)
				return
			}
			// Serve the cached URL rather than failing the playback.
			log.Printf("play %s: refresh failed (%v), using cached URL", room, err)
		} else {
			store.Put(fresh)
			entry = storeEntry{channel: fresh, fetchedAt: time.Now()}
		}
	}
	http.Redirect(w, r, entry.channel.URL, http.StatusFound)
}

func atomicWrite(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".iptv-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0644); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
