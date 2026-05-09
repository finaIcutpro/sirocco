package ratelimit

import "strings"

func Heuristic(route string) (int, float64, bool) {
	route = canonical(route)

	switch {
	case strings.HasPrefix(route, "POST /channels/:id/messages"):
		return 5, 5, true
	case strings.HasPrefix(route, "PATCH /channels/:id/messages"):
		return 5, 5, true
	case strings.HasPrefix(route, "DELETE /channels/:id/messages/:id"):
		return 5, 5, true
	case strings.HasPrefix(route, "POST /channels/:id/messages/bulk-delete"):
		return 1, 3, true
	case strings.Contains(route, "/reactions/"):
		return 1, 0.25, true
	case strings.HasPrefix(route, "POST /channels/:id/typing"):
		return 10, 60, true
	case strings.HasPrefix(route, "POST /channels/:id/threads"):
		return 5, 60, true
	case strings.HasPrefix(route, "PATCH /channels/:id"):
		return 2, 600, true
	case strings.HasPrefix(route, "PATCH /guilds/:id/members/@me/nick"):
		return 1, 1, true
	case strings.HasPrefix(route, "PATCH /guilds/:id/members/"):
		return 10, 10, true
	case strings.HasPrefix(route, "PUT /guilds/:id/members/"):
		return 10, 10, true
	case strings.HasPrefix(route, "DELETE /guilds/:id/members/"):
		return 10, 10, true
	case strings.Contains(route, "/guilds/:id/members/:id/roles"):
		return 10, 10, true
	case route == "PATCH /guilds/:id":
		return 2, 600, true
	case strings.Contains(route, "/guilds/:id/emojis"):
		return 1, 10, true
	case strings.Contains(route, "/guilds/:id/stickers"):
		return 1, 10, true
	case strings.HasPrefix(route, "GET /guilds/:id/members"):
		return 1, 30, true
	case strings.HasPrefix(route, "POST /guilds/:id/prune"):
		return 1, 10, true
	case strings.HasPrefix(route, "POST /guilds/:id/roles"):
		return 5, 60, true
	case strings.HasPrefix(route, "PATCH /guilds/:id/roles"):
		return 5, 60, true
	case strings.HasPrefix(route, "DELETE /guilds/:id/roles"):
		return 5, 60, true
	case strings.HasPrefix(route, "GET /users/"):
		return 45, 60, true
	case strings.HasPrefix(route, "PATCH /users/@me"):
		return 2, 600, true
	case strings.HasPrefix(route, "POST /users/@me/channels"):
		return 10, 60, true
	case strings.HasPrefix(route, "POST /webhooks/:id/"):
		return 30, 60, true
	case strings.HasPrefix(route, "PATCH /webhooks/:id/"):
		return 10, 60, true
	case strings.HasPrefix(route, "DELETE /webhooks/:id/"):
		return 10, 60, true
	case strings.HasPrefix(route, "POST /interactions/:id/:id/callback"):
		return 50, 1, true
	case strings.HasPrefix(route, "GET /invites/"):
		return 5, 5, true
	case strings.HasPrefix(route, "POST /stage-instances"):
		return 5, 60, true
	case strings.HasPrefix(route, "PATCH /stage-instances/"):
		return 5, 60, true
	case strings.HasPrefix(route, "DELETE /stage-instances/"):
		return 5, 60, true
	case strings.HasPrefix(route, "POST /stickers"):
		return 1, 10, true
	case strings.HasPrefix(route, "PATCH /stickers/"):
		return 1, 10, true
	case strings.HasPrefix(route, "DELETE /stickers/"):
		return 1, 10, true
	default:
		return 0, 0, false
	}
}

func canonical(route string) string {
	method, path, ok := strings.Cut(route, " ")
	if !ok {
		return route
	}
	parts := strings.Split(path, "/")
	for i, segment := range parts {
		if snowflake(segment) {
			parts[i] = ":id"
		}
	}
	return method + " " + strings.Join(parts, "/")
}
