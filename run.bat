@echo off

call gcloud composer environments run sabre-composer-demo^
    --location us-central1^
    trigger_dag -- archive_table__archival_test
