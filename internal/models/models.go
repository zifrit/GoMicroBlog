package models

// User is a registered microblog user.
type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

// Post is a message published by a user.
type Post struct {
	ID     int      `json:"id"`
	Author *User    `json:"author"`
	Text   string   `json:"text"`
	Likes  []string `json:"likes"`
}
