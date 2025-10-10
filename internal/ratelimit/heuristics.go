package ratelimit

import "strings"

// Heuristic returns a conservative capacity and window (seconds) for a normalized route.
// Returns (cap, windowSec, ok).
func Heuristic(normalizedRoute string) (int, float64, bool) {
	route := canonicalRoute(normalizedRoute)

	switch {
	case strings.HasPrefix(route, "POST /channels/:id/messages"):
		return 5, 5.0, true // 5 per 5s per channel
	case strings.HasPrefix(route, "PATCH /channels/:id/messages"):
		return 5, 5.0, true // edits share send bucket
	case strings.HasPrefix(route, "DELETE /channels/:id/messages/:id"):
		return 5, 5.0, true // deletions share send bucket
	case strings.HasPrefix(route, "POST /channels/:id/messages/bulk-delete"):
		return 1, 3.0, true // ~1 / 3s per channel
	case strings.Contains(route, "/reactions/"):
		// PUT or DELETE reactions
		return 1, 0.25, true // ~4/s per channel
	case route == "PATCH /channels/:id":
		return 2, 600.0, true // ~2 per 10min per channel (rename/topic)

	// Threads and typing notifications are fairly loose but still per channel
	case strings.HasPrefix(route, "POST /channels/:id/threads"):
		return 5, 60.0, true // observed bucket ~5/min per channel
	case strings.HasPrefix(route, "POST /channels/:id/typing"):
		return 10, 60.0, true // typing indicators limited per channel

	// Guild-level
	case strings.HasPrefix(route, "PATCH /guilds/:id/members/@me/nick"):
		return 1, 1.0, true // 1/s per guild for nickname change
	case strings.HasPrefix(route, "PATCH /guilds/:id/members/"):
		return 10, 10.0, true // 10 per 10s per guild for member mods
	case strings.HasPrefix(route, "PUT /guilds/:id/members/"):
		return 10, 10.0, true // invites/add member share member mod bucket
	case strings.HasPrefix(route, "DELETE /guilds/:id/members/"):
		return 10, 10.0, true // kicks share member mod bucket
	case strings.Contains(route, "/guilds/:id/members/:id/roles"):
		return 10, 10.0, true // role modifications share member bucket
	case route == "PATCH /guilds/:id":
		return 2, 600.0, true // ~2 per 10min per guild (rename/settings)
	case strings.Contains(route, "/guilds/:id/emojis"):
		return 1, 10.0, true // conservative; Discord is strict here
	case strings.Contains(route, "/guilds/:id/stickers"):
		return 1, 10.0, true // sticker operations similar to emoji

	// Webhooks
	case strings.HasPrefix(route, "POST /webhooks/:id/"):
		return 30, 60.0, true // ~30/min per webhook
	case strings.HasPrefix(route, "PATCH /webhooks/:id/"):
		return 10, 60.0, true // editing webhook messages is looser but shared bucket

	// Interactions
	case strings.HasPrefix(route, "POST /interactions/:id/:id/callback"):
		return 50, 1.0, true // callbacks must respond quickly; rely on global limit mostly

	// Misc read endpoints
	case strings.HasPrefix(route, "GET /users/"):
		return 45, 60.0, true // ~45/min per user
	case strings.HasPrefix(route, "GET /guilds/:id/members"):
		return 1, 30.0, true // 1 per 30s per guild
	}
	return 0, 0, false
}

func canonicalRoute(route string) string {
	if route == "" {
		return route
	}
	parts := strings.SplitN(route, " ", 2)
	if len(parts) != 2 {
		return route
	}
	pathParts := strings.Split(parts[1], "/")
	for i, segment := range pathParts {
		if isSnowflake(segment) {
			pathParts[i] = ":id"
		}
	}
	parts[1] = strings.Join(pathParts, "/")
	return parts[0] + " " + parts[1]
}
