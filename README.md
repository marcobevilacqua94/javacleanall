code to test java bulk reactive transaction forced cleanup

the script resolves any transaction found in the system, by either cancelling it if it was not committed or finish committing it if it was committed
use it like this 

java -jar JavaTest.jar localhost Administrator password 


localhost is the couchbase host 

Administrator is the couchbase username

password is the couchbase password

