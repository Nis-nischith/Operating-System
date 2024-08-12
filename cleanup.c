#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>

#define PERMS 0644 // 644 means you can read and write the file or directory and other users can only read it
#define SIZE 50

int msqid; // it is the identifier of the message queue that is created or accessed by my program
key_t key; // used to store the unique key that identifies the message queue
// The key is used to create or access the message queue, while the identifier (msqid) is used to interact with the specific message queue once it has been created or opened.

struct my_msg
{                     // this structure used when dealing with message queues
    long m_type;      // operation to perform;
    char mtext[SIZE]; // while sending holds the filename and while receiving it holds the result
};

int main()
{
    struct my_msg message;
    int msqid;
    key_t key;

    if ((key = ftok("load_balancer.c", 'B')) == -1)
    {                                         // ftok generates a unique key for the message queue. ftok takes in a filename and project identifier(B here)
        perror("Error while creating key\n"); // if it fails to create a key, it prints an error message
        // error when file dne, file is a directory, permission denied, etc.
        exit(-1); // exit is used to terminate a program and return a status code to OS..exit(0) =>successful, non zero exit => error or some abnormal condition
    }
    if ((msqid = msgget(key, PERMS)) == -1)
    {
        perror("Error while creating message queue\n");
        exit(-2);
    }

    message.m_type = 1;
    message.mtext[3] = 53;
    message.mtext[0] = 't';
    while (1)
    {
        puts("Want to terminate the application? Press Y (Yes) or N (No)");
        char c[10];
        scanf("%s", c);
        if (strncmp(c, "Y", 10) == 0)
        {
            printf("\nTerminated");
            if (msgsnd(msqid, &message, SIZE, 0) == -1)
            {
                perror("Error while sending message\n");
                exit(-3);
            }
            break;
        }
    }
    return 0;
}
