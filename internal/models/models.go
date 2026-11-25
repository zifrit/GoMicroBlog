package models

// User описывает зарегистрированного пользователя MicroBlog.
type User struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

// Post описывает сообщение, опубликованное пользователем.
type Post struct {
	ID     int      `json:"id"`
	Author *User    `json:"author"`
	Text   string   `json:"text"`
	Likes  []string `json:"likes"`
}
