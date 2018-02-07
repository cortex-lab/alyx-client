# alyx-client (work in progress)

The Alyx Python client provides a simple API and command-line interface to query the Alyx server via the REST API, and to manage file transfers with Globus.

## Examples

```bash
$ alyx login
Username: ...
Password: ...

$ # All REST endpoints work out of the box.
$ alyx get /sessions
$ alyx get "/subjects/CBGADCre1/datasets?filter=value"
$ alyx post /datasets name=...
$ alyx patch /subject/uuid field=value
...

# Transfer a file record from another using globus.
$ alyx transfer <source_file_record_uuid> <destination_file_record_uuid>

# Transfer all file records associated to a dataset (assuming there is at least one file record
# on a non-personal globus endpoint).
$ alyx transfer --dataset <dataset_uuid>

# Transfer all file records that are missing.
$ alyx transfer --all

# See the status of a Globus file transfer task.
$ alyx status <task_uuid>

# Update the exists field of all missing file records associated to a dataset, by using globus
# to query the state of the file on every data repository.
$ alyx sync <dataset_uuid>

# Update the exists field of all missing file records.
$ alyx sync --all
```
