# README

## Setup / Development
### Java
`17.0.5-amzn` see sdkman on how to install this.  Using the latest mvn seems ok.

### Rebuilding the connector
`make build`

### Gsheet
- need to follow google's guide setup a service account (to get the service_account.json)
- need to go to each sheet to add the service account email in the list of editors

## Testing
- `make build up` to spin up a docker container (it is a worker/coordinator all in one.  not in detached mode to see log messages)
- `make cli` in a separate terminal.
  Run queries like `insert into test_table1(number, text) values('6', 'six');
