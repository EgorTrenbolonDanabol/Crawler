package main

// import (
// 	"context"
// 	crawler "crawler/internal/filecrawler"
// 	"crawler/internal/fs"
// 	"fmt"
// 	"os"
// 	"time"
// )

// func main() {
// 	// Контекст с таймаутом, чтобы гарантировать завершение
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	// Файловая система из твоего пакета (или os.DirFS как временный вариант)
// 	fileSystem := fs.NewOsFileSystem() // предполагаю, что у тебя есть такая реализация
// 	root := "."                        // стартовая директория

// 	// Канал для ошибок
// 	errorChan := make(chan error)

// 	// Создаём краулер
// 	c := crawler.New[any, any]()

// 	// Запускаем поиск
// 	filesChan := c.Search(ctx, fileSystem, root, 4, errorChan)

// 	// Читаем результаты и ошибки
// 	go func() {
// 		for err := range errorChan {
// 			fmt.Fprintf(os.Stderr, "ошибка: %v\n", err)
// 		}
// 	}()

// 	for file := range filesChan {
// 		fmt.Println("Найден файл:", file)
// 	}
// }
