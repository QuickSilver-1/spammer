package main

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"
)

func RunPipeline(cmds ...cmd) {

	in := make(chan interface{})
	var out chan interface{}

	for _, com := range cmds {
		out = make(chan interface{})
		go com(in, out)
		in = out
	}

	time.Sleep(time.Second*3)
}

func SelectUsers(in, out chan interface{}) {
	var differentUser []User
	mu := &sync.Mutex{}

	for email := range in {
		go func(email string) {
				user := GetUser(email)
				if !slices.Contains(differentUser, user) {
					mu.Lock()
					differentUser = append(differentUser, user)
					mu.Unlock()
					out <- user
			}
		}(email.(string))
	}
}

func SelectMessages(in, out chan interface{}) {

	for firstEmail := range in {
		
		secondEmail, err := <-in
		var msg []MsgID

		if !err {
			msg, _ = GetMessages(firstEmail.(User))
		} else {
			msg, _ = GetMessages(firstEmail.(User), secondEmail.(User))
		}

		for _, i := range msg {
			out <- i
		}
	}
}

func CheckSpam(in, out chan interface{}) {
	antiDdos := make(chan interface{}, 5)

	for msg := range in {
		
		antiDdos <- msg
		go func(msg MsgID) {
			spam, _ := HasSpam(msg)
			<- antiDdos

			msgData := MsgData{ID: msg, HasSpam: spam}
			out<- msgData
		}(msg.(MsgID))
	}
}

func CombineResults(in, out chan interface{}) {
	var spamMsg []string
	var noSpamMsg []string

	for msgData := range in {
		fmt.Println(msgData)
		if msgData.(MsgData).HasSpam {
			id := msgData.(MsgData).ID
			spamMsg = append(spamMsg, strconv.FormatUint(uint64(id), 10))
		} else {
			id := msgData.(MsgData).ID
			noSpamMsg = append(noSpamMsg, strconv.FormatUint(uint64(id), 10))
		}
	}

	sort.Strings(spamMsg)
	sort.Strings(noSpamMsg)

	for _, i := range noSpamMsg {
		out<- i
	}
	for _, i := range spamMsg {
		out<- i
	}
}

// инициализация джобы, которая просто выплюнет в out подряд все из слайса строк strs
func newCatStrings1(strs []string, pauses time.Duration) func(in, out chan interface{}) {
	return func(in, out chan interface{}) {
		for _, email := range strs {
			out <- email
			if pauses != 0 {
				time.Sleep(pauses)
			}
		}
	}
}

// инициализация джобы, которая считает из in все строки, пока канал не закроется. и положит все в strs
func newCollectStrings1(in, out chan interface{}) {
	for dataRaw := range in {
		data := fmt.Sprintf("%v", dataRaw)
		strs := data
		fmt.Println(strs)
		out<- 1
	}
}

func main() {

inputData := []string{
		"harry.dubois@mail.ru",
		"k.kitsuragi@mail.ru",
		"d.vader@mail.ru",
		"noname@mail.ru",
		"e.musk@mail.ru",
		"spiderman@mail.ru", // is an alias for peter.parker@mail.ru
		"red_prince@mail.ru",
		"tomasangelo@mail.ru",
		"batman@mail.ru", // is an alias for bruce.wayne@mail.ru
		"bruce.wayne@mail.ru",
	}

	stat = Stat{}
	RunPipeline(
		cmd(newCatStrings1(inputData, 0)),
		cmd(SelectUsers),
		cmd(SelectMessages),
		cmd(CheckSpam),
		cmd(CombineResults),
		cmd(newCollectStrings1),
	)

}