import java.util.Scanner;

public class ChatProgram {
    private static KafkaConnection connection;
    private static Scanner scanner;
    private static String currentRoom;
    private static String userId;

    private static void Login() {
        System.out.print("kafkaOtalk> ID: ");
        userId = scanner.nextLine();
        connection.setConsumer(userId);
        connection.setProducer(userId);
        System.out.println();
    }

    private static void ShowLoginMenu() {
        System.out.println("Welcome to KafkaOtalk");
        System.out.println("1. Log In");
        System.out.println("2. Exit");
        System.out.println();
    }
    public static void LoginWindow() {
        int op;

        ShowLoginMenu();

        while (true) {
            System.out.print("kafkaOtalk> ");
            try {
                op = Integer.parseInt(scanner.nextLine());
            } catch (Exception e) {
                System.out.println("Input must be integer from 1 to 2");
                continue;
            }
            if (op == 1) { /* Log in */
                Login();
                ChattingWindow();
                ShowLoginMenu();
            } else if (op == 2) { /* Exit */
                return;
            } else {
                System.out.println("Input must be integer from 1 to 2");
            }
        }
    }

    public static void MakeChatRoom() {
        System.out.print("kafkaOtalk> Chat room name: ");
        String room = scanner.nextLine();
        if (connection.GetSubscribedTopics().contains(room)) {
            System.out.printf("Chat room %s already exist!\n", room);
        } else {
            try {
                connection.CreateTopic(room);
                connection.SubscribeTopic(room);
                System.out.printf("\"%s\" is created!\n",room);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean JoinChatRoom() {
        System.out.print("kafkaOtalk> Chat room name: ");
        String room = scanner.nextLine();
        if (connection.GetSubscribedTopics().contains(room)) {
            currentRoom = room;
            return true;
        } else {
            System.out.printf("Chat room %s doesn't exist!\n", room);
            return false;
        }
    }
    private static void ShowChattingMenu() {
        System.out.println("Chatting Window");
        System.out.println("1. List");
        System.out.println("2. Make");
        System.out.println("3. Join");
        System.out.println("4. Log out");
        System.out.println();
    }
    public static void ChattingWindow() {
        int op;

        ShowChattingMenu();

        while (true) {
            System.out.print("kafkaOtalk> ");
            try {
                op = Integer.parseInt(scanner.nextLine());
            } catch (Exception e) {
                System.out.println("Input must be integer from 1 to 2");
                continue;
            }
            if (op == 1) { /* List */
                for (String topic : connection.GetSubscribedTopics()) {
                    System.out.println(topic);
                }
            } else if (op == 2) { /* Make */
                MakeChatRoom();
            } else if (op == 3) { /* Join */
                if (JoinChatRoom()){
                    ChatRoomWindow();

                    ShowChattingMenu();
                }
            } else if (op == 4) { /* Log out */
                connection.SaveOffsetInfo();
                return;
            } else {
                System.out.println("Input must be integer from 1 to 4");
            }
        }
    }

    public static void WriteMessage() {
        System.out.print("kafkaOtalk> Text: ");
        String msg = scanner.nextLine();
        try {
            connection.Produce(currentRoom, userId, msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void ChatRoomWindow() {
        int op;

        System.out.println(currentRoom);
        System.out.println("1. Read");
        System.out.println("2. Write");
        System.out.println("3. Reset");
        System.out.println("4. Exit");
        System.out.println();

        while (true) {
            System.out.print("kafkaOtalk> ");
            try {
                op = Integer.parseInt(scanner.nextLine());
            } catch (Exception e) {
                System.out.println("Input must be integer from 1 to 2");
                continue;
            }
            if (op == 1) { /* Read */
                var result = connection.Consume(currentRoom);
                for (var record : result) {
                    System.out.printf("%s: %s\n", record.key(), record.value());
                }
            } else if (op == 2) { /* Write */
                WriteMessage();
            } else if (op == 3) { /* Reset */
                connection.SeekToBeginning(currentRoom);
            } else if (op == 4) { /* Exit */
                return;
            } else {
                System.out.println("Input must be integer from 1 to 4");
            }
        }
    }

    public static void main(String[] args) {
        connection = new KafkaConnection();
        scanner = new Scanner(System.in);
        LoginWindow();
    }
}
