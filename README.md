# Webandco.MigrateDatabase

Neos Flow plugin to run doctrine migrations and copy rows from a source to a destination database.

### Table of contents

* [Quickstart](#quickstart)
* [Introduction](#introduction)
* [Command Line Interface](#command-line-interface)
* [Performance](#performance)
* [License](#license)

## Quickstart

1. **Install this package using composer:**

  ```
  composer require webandco/flow-migrate-database
  ```
(or by adding the dependency to the composer manifest of an installed package)

2. **Configure source and destination database**

 ```yaml
  Webandco:
    MigrateDatabase:
      connections:
        source:
          persistence:
            doctrine:
              # secondLevelCache is needed by the entitymanager
              secondLevelCache:
                enable: false
            backendOptions:
              driver: pdo_mysql
              dbname: docker
              user: someuser
              password: somepassword
              host: mysql
              charset: 'UTF8'
              defaultTableOptions:
                charset: 'UTF8'
        destination:
          persistence:
            doctrine:
              # secondLevelCache is needed by the entitymanager
              secondLevelCache:
                enable: false
            backendOptions:
              driver: pdo_pgsql
                dbname: docker
                user: user
                password: pwd
                host: postgres
                charset: 'UTF8'
                defaultTableOptions:
                  charset: 'UTF8'
  ```

3. **Configure commands to create table structure**

 ```yaml
   Webandco:
     MigrateDatabase:
       preprocess:
         commands:
           doctrineMigrate:
             command: 'doctrine:migrate'
           someFlowpackJobQueue:
             command: 'queue:setup'
             arguments:
               queue: 'jobQueueName'
 ```

4. **Create table structure in destination**

  ```
  ./flow migration:createStructure
  ```

5. **Copy rows from source to destination**

  ```
  ./flow migration:copyTables
  ```

## Introduction

This package is meant to be installed temporarily for as long as needed to finish the migration.
The migration is split in two stages:
1. Create the destination table structures, e.g. by using the command `doctrine:migrate` on the destination database
2. Copy the rows from the source database to the destination

For 1. to work on the destination database a custom `EntityManagerFactory` is used
which selects database connection settings based on an environment variable.

This package does not interfere with the configured database in `Neos.Flow.persistence`, but uses custom settings
for the source and destination database.  
Those custom datbase settings have the same configuration options as to those of `Neos.Flow.persistence`.

## Command Line Interface

Use the `webandco.migratedatabase:migration:*` commands to run the migration:

| Command                    | Description                                                                            |
| -------------------------- |----------------------------------------------------------------------------------------|
| migration:createStructure  | Run configured Webandco.MigrateDatabase.structure.commands on the destination database |
| migration:copytables       | Copy rows from the source to the destination database and update sequences as needed   |

## Performance

The insert is split into chunks and sent via `INSERT INTO [table] (col1,col2,....) VALUES (...),(...),(...),..` to the destination database.  
A local test migration of around 2.6 mio rows from MySql to PostgreSQL took around 5 mins.

## License

This package is licensed under the MIT license
