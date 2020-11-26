# KafkaOTalk
> ####  Messenger Program using Apache Kafka

This program connects kafka server at `localhost:9092`      
you should run `Apache Zookeepr` and `Kafka server` in `localhost` to run this program.
##### There are some issues running Kafka server in Windows. I highly recommend using linux.

### How to Build & Run program
This is Maven Project. Created by IntelliJ.  
This program uses Apache Kafka for message queueing system,  
So you need to run Apache Kafka server at `localhost:9092` to run program appropriately. (Also `Zookeeper`)   
To Build & Run Program, follow under descriptions.  
1. Open Current Directory with IntelliJ IDEA.  
2. Open `Build` Tab in Status Bar (Top of screen), and Click `Build Project`.
3. **RUN ZOOKEEPER AND KAFKA SERVER**
4. Run `ChatProgram` after build finishes. 

### Program Manual  
Chat Program has Three windows.  
Check Descriptions of each windows below.

---
1. Log in window
    - type 1 or 2 to select operation.
    - 1 : Log in
        - Log in chat program with typed uesername.
    - 2 : Exit 
        - Terminate chatting program
2. Chatting window 
    - type integer from 1 to 4 to select operation.
    - 1 : List
        - List all Chat rooms. 
    - 2 : Make
        - Make new Chat room.
    - 3 : Join
        - Join Chat room (must create before join chatroom)
    - 4 : Log Out
        - Log Out and go back to log in window. 
3. Chat room window
    - 1 : Read
        - Read all unread messages.
    - 2 : Write
        - Write message.
    - 3 : Reset
        - Reset chat room. After reset, you can read from first message.
    - 4 : Exit
        - Exit to chatting window
---
##### Log in window example
```
Welcome to KafkaOtalk
1. Log In
2. Exit

kafkaOtalk> 1
kafkaOtalk> ID: angjin

Welcome to KafkaOtalk
1. Log In
2. Exit

kafkaOtalk> 2

Process finished with exit code 0
```

##### Chatting window example
```
Chatting Window
1. List
2. Make
3. Join
4. Log out

kafkaOtalk> 1
kafkaOtalk> 2
kafkaOtalk> Chat room name: kafkarot
"kafkarot" is created!
kafkaOtalk> 1
kafkarot
kafkaOtalk> 3
kafkaOtalk> Chat room name: kafkarot
```

##### Chat room window example
```
kafkarot
kafkaOtalk> 3
kafkaOtalk> Chat room name: kafkarot
kafkarot
1. Read
2. Write
3. Reset
4. Exit

kafkaOtalk> 1
kafkaOtalk> 2
kafkaOtalk> Text: Ovan! Blow whistle!
kafkaOtalk> 1
angjin: Ovan! Blow whistle!
kafkaOtalk> 1
kafkaOtalk> 3
kafkaOtalk> 1
angjin: Ovan! Blow whistle!

// After log in with user id : ovan

Chatting Window
1. List
2. Make
3. Join
4. Log out

kafkaOtalk> 2
kafkaOtalk> Chat room name: kafkarot
"kafkarot" is created!
kafkaOtalk> 3
kafkaOtalk> Chat room name: kafkarot

kafkarot
1. Read
2. Write
3. Reset
4. Exit

kafkaOtalk> 1
angjin: Ovan! Blow whistle!
kafkaOtalk> 2
kafkaOtalk> Text: whi~ whiwhiwhi~
kafkaOtalk> 3
kafkaOtalk> 1
angjin: Ovan! Blow whistle!
ovan: whi~ whiwhiwhi~
```