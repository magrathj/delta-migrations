# delta-migrations
Pyspark package to handle schema migrations in larger scale projects where incremental changes to tables are required. 

[![delta-migration package](https://github.com/magrathj/delta-migrations/actions/workflows/python-package.yml/badge.svg)](https://github.com/magrathj/delta-migrations/actions/workflows/python-package.yml)

## Delta Migrations

Delta migrations is a way of propagating changes you make to your models (adding a field, deleting a model, etc.) into your database schema. They’re designed to be mostly automatic, but you’ll need to know when to make migrations, when to run them, and the common problems you might run into. It follows a similar setup to that of the Django's Database migrations. 

### The Commands

There are several commands which you will use to interact with migrations and Delta’s handling of database schema:

    create_migration_dir, creates a directory to hold your delta migration scripts with a downloaded template scripts.
    

**Example**
```
    delta_migrations create-migration-dir /tmp/delta_migration/
```


You should think of migrations as a version control system for your database schema.

The migration files for each app live in a “migrations” directory inside of that app, and are designed to be committed to, and distributed as part of, its codebase. You should be making them once on your development machine and then running the same migrations on your colleagues’ machines, your staging machines, and eventually your production machines.

### Developer Setup

```
 git config user.name "John Doe"
 git config user.email johndoe@example.com
```

```
    pip install -r requirements.txt
```

```
    pip install -e .
```

```
    python -m pytest --cov=delta_migrations tests/
```

Note

![image](https://user-images.githubusercontent.com/26692441/136924716-bd01a465-3725-47ab-94ab-ed04af9bd0f5.png)
