# delta-migrations
Pyspark package to handle schema migrations in larger scale projects where incremental changes to tables are required. 

[![delta-migration package](https://github.com/magrathj/delta-migrations/actions/workflows/python-package.yml/badge.svg)](https://github.com/magrathj/delta-migrations/actions/workflows/python-package.yml)

## Delta Migrations

Delta migrations is a way of propagating changes you make to your models (adding a field, deleting a model, etc.) into your database schema. They’re designed to be mostly automatic, but you’ll need to know when to make migrations, when to run them, and the common problems you might run into. It follows the same setup as Django's migrations. 

### The Commands

There are several commands which you will use to interact with migrations and Django’s handling of database schema:

    migrate, which is responsible for applying and unapplying migrations.
    makemigrations, which is responsible for creating new migrations based on the changes you have made to your models.
    sqlmigrate, which displays the SQL statements for a migration.
    showmigrations, which lists a project’s migrations and their status.

You should think of migrations as a version control system for your database schema. makemigrations is responsible for packaging up your model changes into individual migration files - analogous to commits - and migrate is responsible for applying those to your database.

The migration files for each app live in a “migrations” directory inside of that app, and are designed to be committed to, and distributed as part of, its codebase. You should be making them once on your development machine and then running the same migrations on your colleagues’ machines, your staging machines, and eventually your production machines.


### Developer Setup

```
    pip install -r requirements.txt
```

```
    pip install -e .
```


Note

![image](https://user-images.githubusercontent.com/26692441/136924716-bd01a465-3725-47ab-94ab-ed04af9bd0f5.png)
