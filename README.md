# dsg_single_faliure_recovery

Single Faliure Recovery on Distributed Social Graph

To Execute the total system follow the order given below:

Run all the nodes with a I/P dataset:
------------------------------
1:- Save the project in your computer.
2:- Open the terminal(ex: powershell) and change the default execution path to the path where the project is stored.
    (ex: cd D:\Dhiman\viii_sem\project\dsg_single_faliure_recovery\src)
3:- Now run the controller.py with with any csv file as argv input
    (ex:  python .\controller.py '..\data.\sample_20.csv')

To kill a specified node:
------------------------
1:- Open another terminal and change the path to project execution path as previous.
2:- Now run the kill_move.py with the node id you want to kill as argv input.
    (ex:  python .\kill_move.py 15) 
3:- Wait for neighbour nodes to detect the failure.

To Recover the failed node:
--------------------------
1:- Run vertex.py with the failed node id as argv input, in the terminal where project path is set.  
    (ex:  python .\vertex.py ./ 15)
2:- Wait for the node to completely recover.

To Check accuracy of recovery:
-----------------------------
1:- Run accuracy.py with the failed node id as argv input, in the terminal where project path is set.  
    (ex:  python .\accuracy.py ./ 15)
