package ratelimit

import "strings"

// Heuristic returns a conservative capacity and window (seconds)
// for a normalized Discord REST route (for bot tokens).
// Returns (cap, windowSec, ok).
func Heuristic(normalizedRoute string) (int, float64, bool) {
	route := canonicalRoute(normalizedRoute)

	switch {

	// ================================
	// CHANNEL-LEVEL (per-channel)
	// ================================

	case strings.HasPrefix(route, "POST /channels/:id/messages"):
		return 5, 5.0, true // send: 5 per 5s per channel
	case strings.HasPrefix(route, "PATCH /channels/:id/messages"):
		return 5, 5.0, true // edits share send bucket
	case strings.HasPrefix(route, "DELETE /channels/:id/messages/:id"):
		return 5, 5.0, true // deletions share send bucket
	case strings.HasPrefix(route, "POST /channels/:id/messages/bulk-delete"):
		return 1, 3.0, true // bulk delete: 1 per 3s per channel
	case strings.Contains(route, "/reactions/"):
		return 1, 0.25, true // reaction add/remove: ~4/s per channel
	case strings.HasPrefix(route, "POST /channels/:id/typing"):
		return 10, 60.0, true // typing: ~10/min per channel
	case strings.HasPrefix(route, "POST /channels/:id/threads"):
		return 5, 60.0, true // thread creation: 5/min per channel
	case strings.HasPrefix(route, "PATCH /channels/:id"):
		return 2, 600.0, true // rename/topic: 2 per 10min per channel

	// ================================
	// GUILD-LEVEL (per-guild)
	// ================================

	case strings.HasPrefix(route, "PATCH /guilds/:id/members/@me/nick"):
		return 1, 1.0, true // nickname change: 1/s per guild
	case strings.HasPrefix(route, "PATCH /guilds/:id/members/"):
		return 10, 10.0, true // member modify: 10 per 10s per guild
	case strings.HasPrefix(route, "PUT /guilds/:id/members/"):
		return 10, 10.0, true // add member/invite: 10 per 10s per guild
	case strings.HasPrefix(route, "DELETE /guilds/:id/members/"):
		return 10, 10.0, true // kick: 10 per 10s per guild
	case strings.Contains(route, "/guilds/:id/members/:id/roles"):
		return 10, 10.0, true // role add/remove: 10 per 10s per guild
	case route == "PATCH /guilds/:id":
		return 2, 600.0, true // guild rename/settings: 2 per 10min per guild
	case strings.Contains(route, "/guilds/:id/emojis"):
		return 1, 10.0, true // emoji create/delete: 1 per 10s per guild
	case strings.Contains(route, "/guilds/:id/stickers"):
		return 1, 10.0, true // stickers same as emoji
	case strings.HasPrefix(route, "GET /guilds/:id/members"):
		return 1, 30.0, true // list guild members: 1 per 30s per guild
	case strings.HasPrefix(route, "POST /guilds/:id/prune"):
		return 1, 10.0, true // prune members: 1 per 10s per guild
	case strings.HasPrefix(route, "POST /guilds/:id/roles"):
		return 5, 60.0, true // create roles: 5/min per guild
	case strings.HasPrefix(route, "PATCH /guilds/:id/roles"):
		return 5, 60.0, true // edit roles: 5/min per guild
	case strings.HasPrefix(route, "DELETE /guilds/:id/roles"):
		return 5, 60.0, true // delete roles: 5/min per guild

	// ================================
	// USER / DM / RELATIONSHIP-LEVEL
	// ================================

	case strings.HasPrefix(route, "GET /users/"):
		return 45, 60.0, true // user lookups: 45/min per user
	case strings.HasPrefix(route, "PATCH /users/@me"):
		return 2, 600.0, true // change username/avatar: 2 per 10min
	case strings.HasPrefix(route, "POST /users/@me/channels"):
		return 10, 60.0, true // create DM: 10/min per user

	// ================================
	// WEBHOOKS
	// ================================

	case strings.HasPrefix(route, "POST /webhooks/:id/"):
		return 30, 60.0, true // webhook send: 30/min per webhook
	case strings.HasPrefix(route, "PATCH /webhooks/:id/"):
		return 10, 60.0, true // webhook edit: ~10/min per webhook
	case strings.HasPrefix(route, "DELETE /webhooks/:id/"):
		return 10, 60.0, true // webhook delete: 10/min per webhook

	// ================================
	// INTERACTIONS
	// ================================

	case strings.HasPrefix(route, "POST /interactions/:id/:id/callback"):
		return 50, 1.0, true // interaction callback: up to 50/s per bot

	// ================================
	// MISCELLANEOUS
	// ================================

	case strings.HasPrefix(route, "GET /invites/"):
		return 5, 5.0, true // invite lookup: 5/5s global
	case strings.HasPrefix(route, "POST /stage-instances"):
		return 5, 60.0, true // stage creation: 5/min per guild
	case strings.HasPrefix(route, "PATCH /stage-instances/"):
		return 5, 60.0, true // edit stage: 5/min per guild
	case strings.HasPrefix(route, "DELETE /stage-instances/"):
		return 5, 60.0, true // delete stage: 5/min per guild
	case strings.HasPrefix(route, "POST /stickers"):
		return 1, 10.0, true // global sticker create
	case strings.HasPrefix(route, "PATCH /stickers/"):
		return 1, 10.0, true // edit sticker
	case strings.HasPrefix(route, "DELETE /stickers/"):
		return 1, 10.0, true // delete sticker

	default:
		return 0, 0, false
	}
}

// canonicalRoute normalizes snowflake IDs to :id placeholders.
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
