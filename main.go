// Написать мини сервис с разделением слоев в одном main.go файле. Сервис должен уметь:
// 1. Подключаться к базе данных
// 2. Использовать кэш c применением Proxy паттерна
// 3. Принимать http запросы REST like API
// 4. Регистрировать пользователя в базе данных
// 5. Выводить список всех пользователей
// 6. У пользователя следующие данные email, password, name, age
// 7. Запретить регистрацию пользователей с одинаковым email и возрастом меньше 18 лет

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/alicebob/miniredis/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/redis/go-redis/v9"

	"database/sql"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := initDB()
	redisClient := initRedis()

	userDb := NewUserDB(ctx, db)
	cache := NewCache(redisClient)
	repo := NewUserRepo(userDb)

	proxyRepo := NewProxyUserRepo(repo, cache)

	userService := NewUserService(proxyRepo)

	userController := NewUserController(userService)

	http.HandleFunc("/users", userController.GetAll)
	http.HandleFunc("/user", userController.Create)

	server := &http.Server{Addr: ":8080"}

	log.Printf("Запуск сервера на порту %s\n", server.Addr)
	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("ошибка запуска http сервера: %s\n", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		log.Println("Получен сигнал завершения работы")
	case <-ctx.Done():
		log.Println("Контекст завершен")
	}

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	if err := server.Shutdown(ctxShutdown); err != nil {
		log.Fatalf("Ошибка при завершении работы сервера: %s", err)
	}

	log.Println("Сервер успешно остановлен")
}

func initDB() *sql.DB {
	db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatalf("ошибка подключения к базе данных: %s\n", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("ошибка подключения к базе данных: %s\n", err)
	}

	log.Println("подключение к базе данных установлено")
	return db
}

func initRedis() *redis.Client {
	s, err := miniredis.Run()
	if err != nil {
		log.Fatalf("ошибка запуска miniredis: %s\n", err)
	}

	log.Println("подключение к redis установлено")

	return redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
}

// /////////////////// models
type User struct {
	Id       int    `json:"id" db:"id,primarykey,autoincrement"`
	Name     string `json:"name" db:"name"`
	Password string `json:"password" db:"password"`
	Email    string `json:"email" db:"email,unique"`
	Age      int    `json:"age" db:"age"`
}

// /////////////////// db
type UserDB interface {
	Create(ctx context.Context, user User) (int, error)
	GetAll(ctx context.Context) ([]User, error)
}

type userDb struct {
	db        *sql.DB
	TableName string
}

func NewUserDB(ctx context.Context, db *sql.DB) UserDB {
	u := &userDb{db: db, TableName: "users"}
	err := u.Migrate(ctx)
	if err != nil {
		log.Fatal(err)
	}

	return u
}

func (u *userDb) Migrate(ctx context.Context) error {
	_, err := u.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT, 
			name VARCHAR(255), 
			password VARCHAR(255), 
			email VARCHAR(255) UNIQUE, 
			age INT
		)`, u.TableName))
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}

func (u *userDb) Create(ctx context.Context, user User) (int, error) {
	result, err := u.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (name, password, email, age) VALUES ($1, $2, $3, $4)", u.TableName),
		user.Name, user.Password, user.Email, user.Age)

	if err != nil {
		return 0, fmt.Errorf("failed to insert user: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve last insert id: %w", err)
	}
	return int(id), nil
}

func (u *userDb) GetAll(ctx context.Context) ([]User, error) {
	rows, err := u.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", u.TableName))
	if err != nil {
		return nil, fmt.Errorf("failed to get all users: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		if err := rows.Scan(&user.Id, &user.Name, &user.Password, &user.Email, &user.Age); err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		users = append(users, user)
	}

	return users, nil
}

// /////////////////// cache
type Cache interface {
	Set(ctx context.Context, key string, value interface{}) error
	Get(ctx context.Context, key string, ptrValue interface{}) error
	Delete(ctx context.Context, key string) error
}

type cache struct {
	client         *redis.Client
	dataExpiration time.Duration
}

func NewCache(cc *redis.Client) Cache {
	defaultDataExpiration := 5 * time.Minute

	return &cache{cc, defaultDataExpiration}
}

func (c *cache) Set(ctx context.Context, key string, value interface{}) error {
	var data []byte
	var ok bool
	if data, ok = value.([]byte); !ok {
		var b bytes.Buffer
		err := json.NewEncoder(&b).Encode(value)
		if err != nil {
			return fmt.Errorf("failed to encode value: %w", err)
		}
		data = b.Bytes()
	}

	err := c.client.Set(ctx, key, data, c.dataExpiration).Err()
	if err != nil {
		log.Printf("ошибка записи в кэш: %s\n", err)
	}

	return nil
}

func (c *cache) Get(ctx context.Context, key string, ptrValue interface{}) error {
	b, err := c.client.Get(ctx, key).Bytes()

	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("key %s not found", key)
		}

		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	buffer := bytes.NewBuffer(b)

	log.Printf("чтение из кэша: %s\n", key)

	err = json.NewDecoder(buffer).Decode(ptrValue)
	if err != nil {
		return fmt.Errorf("failed to decode key %s: %w", key, err)
	}

	return nil
}

func (c *cache) Delete(ctx context.Context, key string) error {
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		log.Printf("ошибка удаления кэша: %s\n", err)
		return err
	}
	return nil
}

// /////////////////// repo
type UserRepo interface {
	Create(ctx context.Context, user User) (int, error)
	GetAll(ctx context.Context) ([]User, error)
}

type userRepo struct {
	db UserDB
}

func NewUserRepo(db UserDB) UserRepo {
	return &userRepo{db}
}

func (ur *userRepo) Create(ctx context.Context, user User) (int, error) {
	return ur.db.Create(ctx, user)
}

func (ur *userRepo) GetAll(ctx context.Context) ([]User, error) {
	return ur.db.GetAll(ctx)
}

// /////////////////// Proxy repo
type proxyUserRepo struct {
	repo  UserRepo
	cache Cache
}

func NewProxyUserRepo(repo UserRepo, cache Cache) UserRepo {
	return &proxyUserRepo{repo, cache}
}

func (p *proxyUserRepo) Create(ctx context.Context, user User) (int, error) {
	id, err := p.repo.Create(ctx, user)
	if err != nil {
		return 0, err
	}

	err = p.cache.Delete(ctx, "users")
	if err != nil {
		log.Printf("ошибка сброса кэша: %s\n", err)
	}

	return id, nil
}

func (p *proxyUserRepo) GetAll(ctx context.Context) ([]User, error) {
	key := "users"

	var users []User
	var err error

	err = p.cache.Get(ctx, key, &users)
	if err == nil {
		return users, nil
	}

	users, err = p.repo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	err = p.cache.Set(ctx, key, users)
	if err != nil {
		log.Print(err)
	}

	return users, nil
}

// /////////////////// service
type UserService interface {
	Create(ctx context.Context, user User) (int, error)
	GetAll(ctx context.Context) ([]User, error)
}

type userService struct {
	repo UserRepo
}

func NewUserService(repo UserRepo) UserService {
	return &userService{repo}
}

func (us *userService) Create(ctx context.Context, user User) (int, error) {
	//email можно поставить как уникальное поле и эта валидация упадет на db
	if user.Age < 18 {
		return 0, fmt.Errorf("возраст пользователя меньше 18 лет")
	}

	return us.repo.Create(ctx, user)
}

func (us *userService) GetAll(ctx context.Context) ([]User, error) {
	return us.repo.GetAll(ctx)
}

// /////////////////// controller
type UserController interface {
	Create(w http.ResponseWriter, r *http.Request)
	GetAll(w http.ResponseWriter, r *http.Request)
}

type userController struct {
	service UserService
}

func NewUserController(service UserService) UserController {
	return &userController{service}
}

func (uc *userController) Create(w http.ResponseWriter, r *http.Request) {
	var user User

	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		log.Printf("ошибка декодирования тела запроса: %s\n", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	id, err := uc.service.Create(r.Context(), user)
	if err != nil {
		log.Printf("ошибка создания юзера: %s\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	log.Printf("новый юзер создан c id: %d\n", id)
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(strconv.Itoa(id)))
}

func (uc *userController) GetAll(w http.ResponseWriter, r *http.Request) {
	users, err := uc.service.GetAll(r.Context())
	if err != nil {
		log.Printf("ошибка получения юзеров: %s\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(users)
	if err != nil {
		log.Printf("ошибка кодирования ответа: %s\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	log.Println("все юзеры получены")
}
