# QUARK

## Build the Project
The Go Programming Language version >=1.21.4  is required to build the project.

Install instructions at <https://go.dev/doc/install>.

Run `go build -o quark` to build the project into an executable.

## Startup
There are two ways to start the project:

### 1. Testing Mode
This mode just starts the tests and prints the results.

Following codes run the optimization 5 times and compares the time difference with the unoptimized version.

Files inside the code folder are needed to run these tests
since they provide the test scenario.
#### Frequent-Neighbours Optimization
```
quark time code/opt1.txt 5
``` 
#### Next-Potential-Caching Optimization
```
quark time code/opt2.txt 5
``` 

### 2. Playground Mode
This is a mode made to be able to manually test the system.

To start in this mode, write `quark DatabaseName.db`.

Following are the commands one can use in this mode
```
write   <file>  <order|optional>
    writes the file in the given filepath
    to the database.
    order in database can be specified if needed

read    <file>
    reads the given file from the database
    and prints the time taken

readio  <file>
    reads the given file from the database
    and writes it to the STDIO.
    not recommended if the file size is big

delete  <file>
    deletes the given file from the database

time    code/<file>    <times|optional>
    runs given file (test case) with 
    and without specified optimization 
    as many times specified.
    prints the time difference.

optimize1
    manually applies the Frequent-Neighbours optimization.
    reorganises the database.

optimize2
    manually toggles the Next-Potential-Caching optimization.

close or exit
    closes the program
```