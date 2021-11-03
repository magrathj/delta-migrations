from delta_migrations.schema_migration_runner import main


def test_main(spark):
    main()