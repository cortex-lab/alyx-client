# alyx-client (work in progress)

The Alyx Python client provides a simple API and command-line interface to interrogate the Alyx server via the REST API.

### Features

* Seamless authentication
* Simple Python API
* Simple command-line interface

### Examples

(work in progress)

Create a `credentials` text file in this directory containing `username:password`. The Alyx client will then handle authentication automatically.

#### Command-line interface

```bash
# All REST endpoints work out of the box.
alyx get /sessions
alyx get /subjects/CBGADCre1/datasets?filter=value
```

Other supported commands include `alyx post`, `alyx put`, `alyx patch`.

#### Python API

```python
c = AlyxClient()
c.post('/path/to', key1=val1, key2=val2)
```
