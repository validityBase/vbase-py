# vBase Python SDK

vbase

A Python library for interacting with the validityBase (vBase) platform

### *class* vbase.ForwarderCommitmentService(forwarder_url: str, api_key: str, private_key: str | None = None, commitment_service_json_file_name: str | None = 'CommitmentService.json')

Bases: `Web3CommitmentService`

Commitment service accessible using a forwarder API endpoint.

#### add_object(object_cid: str) → dict

Record an object commitment.
This is a low-level function that operates on object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  **object_cid** – The CID identifying the object.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set(set_cid: str) → dict

Records a set commitment.
This is a low-level function that operates on set CIDs.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  **set_cid** – The CID identifying the set.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_object(set_cid: str, object_cid: str) → dict

Records a commitment for an object belonging to a set of objects.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cid** – The CID for the set containing the object.
  * **object_cid** – The object hash to record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_objects_batch(set_cid: str, object_cids: List[str]) → List[dict]

Records a batch of commitments for objects belonging to a set.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cid** – The hashes of the sets containing the objects.
  * **object_cids** – The hashes to record.
* **Returns:**
  The commitment logs containing commitment receipts.

#### add_sets_objects_batch(set_cids: List[str], object_cids: List[str]) → List[dict]

Records a batch of commitments for objects belonging to sets.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cids** – The hashes of the sets containing the objects.
  * **object_cids** – The hashes to record.
* **Returns:**
  The commitment logs containing commitment receipts.

#### *static* create_instance_from_env(dotenv_path: str | None = None) → [ForwarderCommitmentService](#vbase.ForwarderCommitmentService)

Creates an instance initialized from environment variables.
Syntactic sugar for initializing new commitment objects using settings
stored in a .env file or in environment variables.

* **Parameters:**
  **dotenv_path** – 

  Path to the .env file.
  Below is the default treatment that should be appropriate in most scenarios:
  - If called with no arguments, or if the default None dotenv_path is specified,
  > default to the existing environment variables.
  - If dotenv_path is specified, attempt to load environment variables from the file.
* **Returns:**
  The dictionary of arguments.

#### get_commitment_service_data() → dict

Get commitment service data from the API server.
This returns the node_rpc_url and the commitment_service_address
for the web3 commitment service abstracted by the forwarder.

* **Returns:**
  The commitment service data for the API server.

#### *static* get_init_args_from_env(dotenv_path: str | None = None) → dict

Worker function to load the environment variables.

* **Parameters:**
  **dotenv_path** – The .env file path, if any.
* **Returns:**
  The dictionary of construction arguments.

#### user_set_exists(user: str, set_cid: str) → bool

Checks whether a given set exists for a user.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **set_cid** – The CID identifying the set.
* **Returns:**
  True if the set exists for the user; False otherwise.

#### verify_user_object(user: str, object_cid: str, timestamp: str) → bool

Verifies an object commitment previously recorded.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **object_cid** – The CID identifying the object.
  * **timestamp** – The timestamp of the commitment.
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

#### verify_user_set_objects(user: str, set_cid: str, user_set_object_cid_sum: str) → bool

Verifies an object commitment previously recorded.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **set_cid** – The CID for the set containing the object.
  * **user_set_object_cid_sum** – The sum of all object hashes for the user set
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

#### verify_user_sets(user: str, user_set_cid_sum: str) → bool

Verifies all set commitments previously recorded by the user.
This verifies all set commitments for completeness.
The sum of all set CIDs for the user encodes the collection of all sets.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **user_set_cid_sum** – The sum of all set CIDs for the user.
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

### *class* vbase.ForwarderCommitmentServiceTest(forwarder_url: str, api_key: str, private_key: str | None = None, commitment_service_json_file_name: str | None = 'CommitmentServiceTest.json')

Bases: [`ForwarderCommitmentService`](#vbase.ForwarderCommitmentService), `CommitmentServiceTest`

Test commitment service accessible using a forwarder API endpoint.

#### add_object_with_timestamp(object_cid: str, timestamp: str) → dict

Test shim to record an object commitment with a given timestamp.
Only supported by test contracts.

* **Parameters:**
  * **object_cid** – The CID identifying the object.
  * **timestamp** – The timestamp to force for the record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_object_with_timestamp(set_cid: str, object_cid: str, timestamp: str) → dict

Test shim to record an object commitment with a given timestamp.
Only supported by test contracts.

* **Parameters:**
  * **set_cid** – The CID of the set containing the object.
  * **object_cid** – The CID to record.
  * **timestamp** – The timestamp to force for the record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_objects_with_timestamps_batch(set_cid: str, object_cids: List[str], timestamps: List[str]) → List[dict]

Test shim to record a batch of object commitment with a timestamps.
Only supported by test contracts.

* **Parameters:**
  * **set_cid** – The CID of the set containing the objects.
  * **object_cids** – The hashes to record.
  * **timestamps** – The timestamps to force for the records.
* **Returns:**
  The commitment logs containing commitment receipts.

#### add_sets_objects_with_timestamps_batch(set_cids: List[str], object_cids: List[str], timestamps: List[str]) → List[dict]

Test shim to record a batch of object commitment with a timestamps.
Only supported by test contracts.

* **Parameters:**
  * **set_cids** – The hashes of the sets containing the objects.
  * **object_cids** – The hashes to record.
  * **timestamps** – The timestamps to force for the records.
* **Returns:**
  The commitment logs containing commitment receipts.

#### clear_set_objects(set_cid: str)

Clear all records (objects) for a user’s set.
Used to clear state when testing.
Only supported by test contracts.

* **Parameters:**
  **set_cid** – Hash identifying the set.

#### clear_sets()

Clear all sets for the user.
Used to clear state when testing.
Only supported by test contracts.

#### *static* create_instance_from_env(dotenv_path: str | None = None) → [ForwarderCommitmentServiceTest](#vbase.ForwarderCommitmentServiceTest)

Creates an instance initialized from environment variables.
Syntactic sugar for initializing new commitment objects using settings
stored in a .env file or in environment variables.

* **Parameters:**
  **dotenv_path** – 

  Path to the .env file.
  Below is the default treatment that should be appropriate in most scenarios:
  - If called with no arguments, or if the default None dotenv_path is specified,
  > default to the existing environment variables.
  - If dotenv_path is specified, attempt to load environment variables from the file.
* **Returns:**
  The dictionary of arguments.

### *class* vbase.IndexingService

Bases: `ABC`

Base indexing operations.
Various indexing services may provide a subset of the below operations that they support.

#### *static* create_instance_from_commitment_service(commitment_service: CommitmentService) → [IndexingService](#vbase.IndexingService)

Creates an instance initialized from a commitment service.
Handles the complexities of initializing an IndexingService
using a forwarded commitment service.
We need to query this service for the information needed to connect to a commitment
service directly, and this method abstracts this initialization.

* **Parameters:**
  **commitment_service** – The commitment service used.
* **Returns:**
  The IndexingService created.

#### *static* create_instance_from_env_json_descriptor(dotenv_path: str | None = None) → [IndexingService](#vbase.IndexingService)

Creates an instance initialized from an environment variable containing a JSON descriptor.
Syntactic sugar for initializing a new indexing service object using settings
stored in a .env file or in environment variables.
This method is especially useful for constructing complex
indexers using multiple commitment service defined using complex JSON.

* **Parameters:**
  **dotenv_path** – Path to the .env file.
  If path is not specified, does not load the .env file.
* **Returns:**
  The IndexingService created.

#### *static* create_instance_from_json_descriptor(is_json: str) → [IndexingService](#vbase.IndexingService)

Creates an instance initialized from a JSON descriptor.
This method is especially useful for constructing complex
indexers using multiple commitment service defined using complex JSON.

* **Parameters:**
  **is_json** – The JSON string with the initialization data.
* **Returns:**
  The IndexingService created.

#### find_last_object(object_cid: str, return_set_cid=False) → dict | None

Returns the last/latest receipt, if any, for object commitments.
Finds and returns individual object commitment irrespective of the set
it may have been committed to.

* **Parameters:**
  * **object_cid** – The CID for the object for search.
  * **return_set_cid** – If True, return the set CIDs, if any, for the object.
* **Returns:**
  The commitment receipt for the last/latest object commitment.

#### find_last_user_set_object(user: str, set_cid: str) → dict | None

Returns the last/latest receipt, if any, for user set object commitments
for a given user and set CID.

* **Parameters:**
  * **user** – The address for the user who made the commitment.
  * **set_cid** – The CID for the set containing the object.
* **Returns:**
  The commitment receipt for the last/latest user set commitment.

#### find_object(object_cid: str, return_set_cids=False) → List[dict]

Returns the list of receipts for object commitments
for a single object CID.
Finds and returns individual object commitments irrespective of the set
they may have been committed to.

* **Parameters:**
  * **object_cid** – The CID for the objects to search.
  * **return_set_cids** – If True, return the set CIDs, if any, for the objects.
* **Returns:**
  The list of commitment receipts for all object commitments.

#### find_objects(object_cids: List[str], return_set_cids=False) → List[dict]

Returns the list of receipts for object commitments
for a list of object CIDs.
Finds and returns individual object commitments irrespective of the set
they may have been committed to.

* **Parameters:**
  * **object_cids** – The CIDs for the objects to search.
  * **return_set_cids** – If True, return the set CIDs, if any, for the objects.
* **Returns:**
  The list of commitment receipts for all object commitments.

#### find_user_objects(user: str, return_set_cids=False) → List[dict]

Returns the list of receipts for user object commitments
for a given user.
Finds and returns individual object commitments irrespective of the set
they may have been committed to.

* **Parameters:**
  * **user** – The address for the user who made the commitments.
  * **object_cids** – The CIDs for the objects to search.
* **Returns:**
  The list of commitment receipts for all user object commitments.

#### find_user_set_objects(user: str, set_cid: str) → List[dict]

Returns the list of receipts for user set object commitments
for a given user and set CID.

* **Parameters:**
  * **user** – The address for the user who made the commitments.
  * **set_cid** – The CID for the set containing the objects.
* **Returns:**
  The list of commitment receipts for all user set object commitments.

#### find_user_sets(user: str) → List[dict]

Returns the list of receipts for user set commitments
for a given user.

* **Parameters:**
  **user** – The address for the user who made the commitments.
* **Returns:**
  The list of commitment receipts for all user set commitments.

### *class* vbase.VBaseClient(commitment_service: CommitmentService)

Bases: `object`

Provides Python validityBase (vBase) access.

#### add_named_set(name: str) → dict

Creates a commitment for a set with a given name.
This function abstracts the low-level commitment of set creation.

* **Parameters:**
  **name** – The name of the set.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_object(object_cid: str) → dict

Record an object commitment.
This is a low-level function that operates on object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  **object_cid** – The CID identifying the object.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set(set_cid: str) → dict

Records a set commitment.
This is a low-level function that operates on set CIDs.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  **set_cid** – The CID (hash) identifying the set.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_object(set_cid: str, object_cid: str) → dict

Records a commitment for an object belonging to a set of objects.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cid** – The CID for the set containing the object.
  * **object_cid** – The object hash to record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_objects_batch(set_cid: str, object_cids: List[str]) → List[dict]

Records a batch of commitments for objects belonging to a set.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cid** – The CID of the set containing the objects.
  * **object_cids** – The hashes to record.
* **Returns:**
  The commitment log list containing commitment receipts.

#### add_sets_objects_batch(set_cids: List[str], object_cids: List[str]) → List[dict]

Records a batch of commitments for objects belonging to sets.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cids** – The hashes of the sets containing the objects.
  * **object_cids** – The hashes to record.
* **Returns:**
  The commitment log list containing commitment receipts.

#### *static* create_instance_from_env(dotenv_path: str | None = None) → [VBaseClient](#vbase.VBaseClient)

Creates an instance initialized from environment variables.
Syntactic sugar for initializing new commitment objects using settings
stored in a .env file or in environment variables.

* **Parameters:**
  **dotenv_path** – 

  Path to the .env file.
  Below is the default treatment that should be appropriate in most scenarios:
  - If called with no arguments, or if the default None dotenv_path is specified,
  > default to the existing environment variables.
  - If dotenv_path is specified, attempt to load environment variables from the file.
* **Returns:**
  The constructed vBase client object.

#### get_default_user() → str

Return the default user address used in vBase transactions.

* **Returns:**
  The default user address used in vBase transactions.

#### get_named_set_cid(name: str) → str

Converts a set name to a hash.
Abstracts the hashing implementation from the upper layers.

* **Parameters:**
  **name** – The name of the set.
* **Returns:**
  The CID for the name.

#### get_sim_t() → Timestamp | None

Get the simulation timestamp.

* **Returns:**
  If in simulation, the sim timestamp; None otherwise.

#### in_sim() → bool

Get the simulation state.

* **Returns:**
  True if vBase is in a simulation; False otherwise.

#### run_pit_sim(ts: DatetimeIndex, callback: Callable[[], int | float | dict | DataFrame]) → Series | DataFrame

Runs a point-in-time (PIT) simulation.
PIT simulation executes callback for each t specified
letting the callback see world state as it existed at that t.

* **Parameters:**
  * **ts** – Times/timestamps for which callback should be called
    and PIT world state simulated.
  * **callback** – The callback to call.
* **Returns:**
  The aggregated output of all callback invocations.

#### user_named_set_exists(user: str, name: str) → bool

Checks whether a set with a given name exists for the calling user.
This function abstracts the low-level commitment of named set creation.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **name** – The name of the set.
* **Returns:**
  True if the set with the given name exists; False otherwise.

#### user_set_exists(user: str, set_cid: str) → bool

Checks whether a given set exists for the calling user.
This function abstracts the low-level commitment of named set creation.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **set_cid** – The CID (hash) identifying the set.
* **Returns:**
  True if the set with the given hash exists; False otherwise.

#### verify_user_named_sets(user: str, names: List[str]) → bool

Verifies the completeness of a list of named sets.

* **Parameters:**
  * **user** – Address for the user who recorded the commitment.
  * **names** – Names of user sets.
* **Returns:**
  True if the names comprise all named sets committed by the user;
  False otherwise.

#### verify_user_object(user: str, object_cid: str, timestamp: Timestamp | str) → bool

Verifies an object commitment previously recorded.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **object_cid** – The CID identifying the object.
  * **timestamp** – The timestamp of the commitment.
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

#### verify_user_set_objects(user: str, set_cid: str, user_set_objects_cid_sum: str) → bool

Verifies an object commitment previously recorded.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **set_cid** – The CID for the set containing the object.
  * **user_set_objects_cid_sum** – The sum of all object hashes for the user set
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

#### verify_user_sets(user: str, user_sets_cid_sum: str) → bool

Verifies set commitments previously recorded by the user.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **user_sets_cid_sum** – The sum of all set CIDs for the user.
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

### *class* vbase.VBaseClientTest(commitment_service: CommitmentServiceTest)

Bases: [`VBaseClient`](#vbase.VBaseClient)

Provides Python validityBase (vBase) access with test methods.
Test methods allow clearing state and bootstrapping objects with pre-defined timestamps.

#### add_object_with_timestamp(object_cid: str, timestamp: Timestamp | str) → dict

Test method to record an object commitment with a given timestamp.
Only supported by test contracts.

* **Parameters:**
  * **object_cid** – The CID identifying the object.
  * **timestamp** – The timestamp to force for the record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_object_with_timestamp(set_cid: str, object_cid: str, timestamp: Timestamp | str) → dict

Test method to record an object commitment with a given timestamp.
Only supported by test contracts.

* **Parameters:**
  * **set_cid** – The CID of the set containing the object.
  * **object_cid** – The CID to record.
  * **timestamp** – The timestamp to force for the record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_objects_with_timestamps_batch(set_cid: str, object_cids: List[str], timestamps: List[Timestamp]) → List[dict]

Test method to record a batch of object commitment with a timestamps.
Only supported by test contracts.

* **Parameters:**
  * **set_cid** – The CID of the set containing the objects.
  * **object_cids** – The hashes to record.
  * **timestamps** – The timestamps to force for the records.
* **Returns:**
  The commitment log list containing commitment receipts.

#### add_sets_objects_with_timestamps_batch(set_cids: List[str], object_cids: List[str], timestamps: List[Timestamp]) → List[dict]

Test method to record a batch of object commitment with a timestamps.
Only supported by test contracts.

* **Parameters:**
  * **set_cids** – The hashes of the sets containing the objects.
  * **object_cids** – The hashes to record.
  * **timestamps** – The timestamps to force for the records.
* **Returns:**
  The commitment log list containing commitment receipts.

#### clear_named_set_objects(name: str)

Clear all records (objects) for a user’s named set.

* **Parameters:**
  **name** – Name of the set to clear.

#### clear_set_objects(set_cid: str)

Clear all records (objects) for a user’s set.
Used to clear state when testing.
Only supported by test contracts.

* **Parameters:**
  **set_cid** – Hash identifying the set.

#### clear_sets()

Clear all sets for the user.
Used to clear state when testing.
Only supported by test contracts.

#### *static* create_instance_from_env(dotenv_path: str | None = None) → [VBaseClientTest](#vbase.VBaseClientTest)

Creates an instance initialized from environment variables.
Syntactic sugar for initializing new commitment objects using settings
stored in a .env file or in environment variables.

* **Parameters:**
  **dotenv_path** – 

  Path to the .env file.
  Below is the default treatment that should be appropriate in most scenarios:
  - If called with no arguments, or if the default None dotenv_path is specified,
  > default to the existing environment variables.
  - If dotenv_path is specified, attempt to load environment variables from the file.
* **Returns:**
  The constructed vBase client object.

#### *static* normalize_pd_timestamp(timestamp: Timestamp | str)

Normalize Pandas timestamp converting it to a string representation
that is serializable.

* **Parameters:**
  **timestamp** – A representation of a pd.Timestamp object.
* **Returns:**
  The string representation of a pd.Timestamp.

### *class* vbase.VBaseDataset(vbc: [VBaseClient](#vbase.VBaseClient) | [VBaseClientTest](#vbase.VBaseClientTest), name: str | None = None, record_type: Type[[VBaseObject](#vbase.VBaseObject)] | None = None, init_dict: dict | None = None, init_json: str | None = None)

Bases: `ABC`

Provides Python vBase dataset access.
Implements base functionality shared across datasets regardless of record type.
Record-specific logic is implemented in the record class.

#### add_record(record_data: any) → dict

Add a record to a VBaseDataset object.

* **Parameters:**
  **record_data** – The record datum.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_record_with_timestamp(record_data: any, timestamp: Timestamp | str) → dict

Test shim to add a record to a VBaseDataset object with a given timestamp.
Only supported by test contracts.

* **Parameters:**
  * **record_data** – The record datum.
  * **timestamp** – Timestamp to force for the record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_records_batch(record_data_list: List[any]) → List[dict]

Add a list of records to a VBaseDataset object.
This function will typically be called to backfill a dataset history:
- Producer creates records for 1/1, 1/2, 1/3.
- On 1/4 methodology changes.
- Producer back-fills history for 1/1, 1/2, 1/3 and adds their commitments.

* **Parameters:**
  **record_data_list** – The list of records’ data.
* **Returns:**
  The commitment log list containing commitment receipts.

#### add_records_with_timestamps_batch(record_data_list: List[any], timestamps: List[Timestamp | str]) → List[dict]

Test shim to add a batch of records with timestamps to a VBaseDataset object.
Only supported by test contracts.

* **Parameters:**
  * **record_data_list** – The list of records’ data.
  * **timestamps** – The list of timestamps to force for the records.
* **Returns:**
  The commitment log list containing commitment receipts.

#### get_commitment_receipts() → List[dict]

Get commitment receipts for dataset records.

* **Returns:**
  Commitment receipts for dataset records.

#### get_last_record() → Any | None

Get the last/latest record for the dataset.

* **Returns:**
  The last/latest record prior to the current time:
  - If not in a simulation, this is the last known record.
  - If within a simulation, this is the last record with a timestamp
  less than or equal to the sim t.
  - If all records are after the current time, returns None.

#### get_last_record_data() → Any | None

Get the last/latest record’s data for the dataset.

* **Returns:**
  The last/latest record data prior to the current time
  using get_last_record() semantics.

#### get_pd_data_frame() → DataFrame | None

Get a Pandas DataFrame representation of the dataset’s records.
This default method works for most datasets.
Datasets that need special handling will override this method.

* **Returns:**
  The pd.DataFrame object representing the dataset’s records.

#### get_records() → List[any] | None

Get all records for the dataset.

* **Returns:**
  All record up to the current time:
  - If not in a simulation, returns all records.
  - If within a simulation, returns records with a timestamp less
  than or equal to the sim t.
  - If all records are after the current time, returns None.

#### *static* get_set_cid_for_dataset(dataset_name: str) → str

Generate set CID for a named dataset.
May be called to post commitments without instantiating a dataset object.

* **Parameters:**
  **dataset_name** – The dataset name.
* **Returns:**
  The CID for the dataset.

#### get_timestamps() → DatetimeIndex

Get all record timestamps.

* **Returns:**
  The timestamps for all dataset records.

#### to_dict() → dict

Return dictionary representation of the dataset.

* **Returns:**
  The dictionary representation of the dataset.

#### to_json() → str

Return JSON representation of the dataset.

* **Returns:**
  The JSON representation of the dataset.

#### try_restore_timestamps_from_index() -> (<class 'bool'>, typing.List[str])

Try to restore timestamps for dataset records using the index service.

The function should always attempt to do the right thing by default,
but long-term options can get complex. The following work remains:
- How should records with identical CIDs be treated?
- When multiple commitments exist for a given CID, what is the order of pairing?

* **Returns:**
  A tuple containing success and log:
  - success: True if all record have been found in the index
  > and timestamps restored; False otherwise.
  - l_log: A list log of verification explaining any failures.

#### verify_commitments() -> (<class 'bool'>, typing.List[str])

Verify commitments for all dataset records.

* **Returns:**
  A tuple containing success and log:
  - success: True if all record commitments have been verified; False otherwise.
  - l_log: A list log of verification explaining any failures.

### *class* vbase.VBaseDatasetAsync(vbc: [VBaseClient](#vbase.VBaseClient) | [VBaseClientTest](#vbase.VBaseClientTest), name: str | None = None, record_type: Type[[VBaseObject](#vbase.VBaseObject)] | None = None, init_dict: dict | None = None, init_json: str | None = None)

Bases: [`VBaseDataset`](#vbase.VBaseDataset)

Provides Python vBase dataset async access.
Asynchronous dataset wraps synchronous dataset object to support
async operations using asyncio.

#### *async* add_record_async(record_data: any) → dict

Add a record to a VBase dataset object asynchronously.
Offloads add_record execution to the default event loop’s executor.

* **Parameters:**
  **record_data** – The record datum.
* **Returns:**
  The commitment log containing commitment receipt info.

#### *async* add_record_with_timestamp_async(record_data: any, timestamp: Timestamp | str) → dict

Test shim to add a record to a VBaseDataset object
with a given timestamp asynchronously.
Only supported by test contracts.
Offloads add_record_with_timestamp execution
to the default event loop’s executor.

* **Parameters:**
  * **record_data** – The record datum.
  * **timestamp** – Timestamp to force for the record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### *async* add_records_batch_async(record_data_list: List[any]) → List[dict]

Add a record to a VBase dataset object asynchronously.
Offloads add_record execution to the default event loop’s executor.

* **Parameters:**
  **record_data_list** – The list of records’ data.
* **Returns:**
  The commitment log list containing commitment receipts.

#### *async* add_records_with_timestamps_batch_async(record_data_list: List[any], timestamps: List[Timestamp | str]) → List[dict]

Test shim to add a batch of records with timestamps
to a VBaseDataset object asynchronously.
Only supported by test contracts.
Offloads add_records_with_timestamps_batch execution
to the default event loop’s executor.

* **Parameters:**
  * **record_data_list** – The list of records’ data.
  * **timestamps** – The list of timestamps to force for the records.
* **Returns:**
  The commitment log list containing commitment receipts.

#### *async classmethod* create(\*args, \*\*kwargs) → [VBaseDatasetAsync](#vbase.VBaseDatasetAsync)

Creates a vBase dataset object asynchronously.
A static async factory method that delegates to the synchronous constructor.
Offloads VBaseDataset constructor execution to the default event loop’s executor.

* **Parameters:**
  * **args** – Arguments passed to the VBaseDataset constructor.
  * **kwargs** – Arguments passed to the VBaseDataset constructor.
* **Returns:**
  The created dataset.

#### *async* verify_commitments_async() -> (<class 'bool'>, typing.List[str])

Verify commitments for all dataset records asynchronously.
Offloads verify_commitments execution
to the default event loop’s executor.

* **Returns:**
  A tuple containing success and log:
  - success: true if all record commitments have been verified; false otherwise
  - l_log: a list log of verification explaining any failures

### *class* vbase.VBaseFloatObject(init_data: float | None = None, init_dict: Dict[str, float] | None = None, init_json: str | None = None)

Bases: [`VBaseObject`](#vbase.VBaseObject)

A float object
Floats are committed as fixed-point integers to support ZKPs.

#### *static* get_cid_for_data(record_data: float) → str

Generate a content identifier (CID) for an object with given data.
The method may be called to post commitments without instantiating an object.
The encapsulation of different digital objects and
their CID calculation is a primary job of an object.

* **Parameters:**
  **record_data** – The object data.
  Allows calculating a CID without instantiating an object.
* **Returns:**
  The CID generated.

### *class* vbase.VBaseIntObject(init_data: int | None = None, init_dict: Dict[str, int] | None = None, init_json: str | None = None)

Bases: [`VBaseObject`](#vbase.VBaseObject)

An integer object

#### *static* get_cid_for_data(record_data: int) → str

Generate a content identifier (CID) for an object with given data.
The method may be called to post commitments without instantiating an object.
The encapsulation of different digital objects and
their CID calculation is a primary job of an object.

* **Parameters:**
  **record_data** – The object data.
  Allows calculating a CID without instantiating an object.
* **Returns:**
  The CID generated.

### *class* vbase.VBaseJsonObject(init_data: str | None = None, init_dict: Dict[str, str] | None = None, init_json: str | None = None)

Bases: [`VBaseObject`](#vbase.VBaseObject)

A JSON string object

#### *static* get_cid_for_data(record_data: str) → str

Generate a content identifier (CID) for an object with given data.
The method may be called to post commitments without instantiating an object.
The encapsulation of different digital objects and
their CID calculation is a primary job of an object.

* **Parameters:**
  **record_data** – The object data.
  Allows calculating a CID without instantiating an object.
* **Returns:**
  The CID generated.

#### get_dict() → dict

Return the dictionary representation of the object’s data.
This is a basic implementation that most objects should override
with more intelligent object-specific implementations.
Converting objects to dictionaries is useful as a step in converting
sets to DataFrames.

* **Returns:**
  The dictionary representation of the object.

### *class* vbase.VBaseObject(init_data: Any | None = None, init_dict: Dict | None = None, init_json: str | None = None)

Bases: `ABC`

Provides basic Python vBase object features.
Implements base functionality shared across various objects and dataset records.
Children implement object-specific logic.

#### cid *: str | None*

#### data *: Any*

#### get_cid() → str

Return the content identifier (CID) for the object.
Calculates the CID if necessary and caches it for subsequent queries.

* **Returns:**
  The CID generated.

#### *abstract static* get_cid_for_data(record_data: Any) → str

Generate a content identifier (CID) for an object with given data.
The method may be called to post commitments without instantiating an object.
The encapsulation of different digital objects and
their CID calculation is a primary job of an object.

* **Parameters:**
  **record_data** – The object data.
  Allows calculating a CID without instantiating an object.
* **Returns:**
  The CID generated.

#### get_dict() → dict

Return the dictionary representation of the object’s data.
This is a basic implementation that most objects should override
with more intelligent object-specific implementations.
Converting objects to dictionaries is useful as a step in converting
sets to DataFrames.

* **Returns:**
  The dictionary representation of the object.

### *class* vbase.VBasePortfolioObject(init_data: Dict[str, int | float] | None = None, init_dict: Dict[str, Dict[str, int | float]] | None = None, init_json: str | None = None)

Bases: [`VBaseObject`](#vbase.VBaseObject)

A portfolio object
Each portfolio is a dictionary with
symbol/id keys and weight values.

#### *static* get_cid_for_data(record_data: dict) → str

Generate a content identifier (CID) for an object with given data.
The method may be called to post commitments without instantiating an object.
The encapsulation of different digital objects and
their CID calculation is a primary job of an object.

* **Parameters:**
  **record_data** – The object data.
  Allows calculating a CID without instantiating an object.
* **Returns:**
  The CID generated.

### *class* vbase.VBasePrivateFloatObject(init_data: float | str | None = None, init_dict: Dict[str, float | str] | None = None, init_json: str | None = None)

Bases: [`VBaseObject`](#vbase.VBaseObject)

A float object that preserves object privacy
Each object comprises a float value and a string salt.

#### *static* get_cid_for_data(record_data: Tuple[int, str]) → str

Generate a content identifier (CID) for an object with given data.
The method may be called to post commitments without instantiating an object.
The encapsulation of different digital objects and
their CID calculation is a primary job of an object.

* **Parameters:**
  **record_data** – The object data.
  Allows calculating a CID without instantiating an object.
* **Returns:**
  The CID generated.

### *class* vbase.VBasePrivateIntObject(init_data: int | str | None = None, init_dict: Dict[str, int | str] | None = None, init_json: str | None = None)

Bases: [`VBaseObject`](#vbase.VBaseObject)

An integer object that preserves object privacy
Each object comprises an integer value and a string salt.
The user-specified random salt preserves privacy of the data with low entropy.
To verify the object, users must specify the preimage with salts.
The source datasets to be validated will be commonly stored as a spreadsheet with two columns.

#### *static* get_cid_for_data(record_data: Tuple[int, str]) → str

Generate a content identifier (CID) for an object with given data.
The method may be called to post commitments without instantiating an object.
The encapsulation of different digital objects and
their CID calculation is a primary job of an object.

* **Parameters:**
  **record_data** – The object data.
  Allows calculating a CID without instantiating an object.
* **Returns:**
  The CID generated.

### *class* vbase.VBaseStringObject(init_data: str | None = None, init_dict: Dict[str, str] | None = None, init_json: str | None = None)

Bases: [`VBaseObject`](#vbase.VBaseObject)

A string object

#### *static* get_cid_for_data(record_data: str) → str

Generate a content identifier (CID) for an object with given data.
The method may be called to post commitments without instantiating an object.
The encapsulation of different digital objects and
their CID calculation is a primary job of an object.

* **Parameters:**
  **record_data** – The object data.
  Allows calculating a CID without instantiating an object.
* **Returns:**
  The CID generated.

### *class* vbase.Web3HTTPCommitmentService(node_rpc_url: str, commitment_service_address: str, private_key: str | None = None, commitment_service_json_file_name: str | None = 'CommitmentService.json', inject_geth_poa_middleware: bool = False)

Bases: `Web3CommitmentService`

Commitment service accessible using Web3.HTTPProvider.
Without private key support, this class will only support operations on a test node.

#### add_object(object_cid: str) → dict

Record an object commitment.
This is a low-level function that operates on object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  **object_cid** – The CID identifying the object.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set(set_cid: str) → dict

Records a set commitment.
This is a low-level function that operates on set CIDs.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  **set_cid** – The CID identifying the set.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_object(set_cid: str, object_cid: str) → dict

Records a commitment for an object belonging to a set of objects.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cid** – The CID for the set containing the object.
  * **object_cid** – The object hash to record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_objects_batch(set_cid: str, object_cids: List[str]) → List[dict]

Records a batch of commitments for objects belonging to a set.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cid** – The hashes of the sets containing the objects.
  * **object_cids** – The hashes to record.
* **Returns:**
  The commitment logs containing commitment receipts.

#### add_sets_objects_batch(set_cids: List[str], object_cids: List[str]) → List[dict]

Records a batch of commitments for objects belonging to sets.
This is a low-level function that operates on set and object hashes.
It does not specify how a hash is built and does not provide
a schema for hashing complex information.

* **Parameters:**
  * **set_cids** – The hashes of the sets containing the objects.
  * **object_cids** – The hashes to record.
* **Returns:**
  The commitment logs containing commitment receipts.

#### *static* create_instance_from_env(dotenv_path: str | None = None) → [Web3HTTPCommitmentService](#vbase.Web3HTTPCommitmentService)

Creates an instance initialized from environment variables.
Syntactic sugar for initializing new commitment objects using settings
stored in a .env file or in environment variables.

* **Parameters:**
  **dotenv_path** – 

  Path to the .env file.
  Below is the default treatment that should be appropriate in most scenarios:
  - If called with no arguments, or if the default None dotenv_path is specified,
  > default to the existing environment variables.
  - If dotenv_path is specified, attempt to load environment variables from the file.
* **Returns:**
  The dictionary of arguments.

#### *static* get_init_args_from_env(dotenv_path: str | None = None) → dict

Worker function to load the environment variables.

* **Parameters:**
  **dotenv_path** – The .env file path, if any.
* **Returns:**
  The dictionary of construction arguments.

#### user_set_exists(user: str, set_cid: str) → bool

Checks whether a given set exists for a user.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **set_cid** – The CID identifying the set.
* **Returns:**
  True if the set exists for the user; False otherwise.

#### verify_user_object(user: str, object_cid: str, timestamp: str) → bool

Verifies an object commitment previously recorded.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **object_cid** – The CID identifying the object.
  * **timestamp** – The timestamp of the commitment.
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

#### verify_user_set_objects(user: str, set_cid: str, user_set_object_cid_sum: str) → bool

Verifies an object commitment previously recorded.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **set_cid** – The CID for the set containing the object.
  * **user_set_object_cid_sum** – The sum of all object hashes for the user set
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

#### verify_user_sets(user: str, user_set_cid_sum: str) → bool

Verifies all set commitments previously recorded by the user.
This verifies all set commitments for completeness.
The sum of all set CIDs for the user encodes the collection of all sets.
This is a low-level function that operates on object hashes.

* **Parameters:**
  * **user** – The address for the user who recorded the commitment.
  * **user_set_cid_sum** – The sum of all set CIDs for the user.
* **Returns:**
  True if the commitment has been verified successfully;
  False otherwise.

### *class* vbase.Web3HTTPCommitmentServiceTest(node_rpc_url: str = None, commitment_service_address: str = None, private_key: str | None = None, commitment_service_json_file_name: str | None = 'CommitmentServiceTest.json', inject_geth_poa_middleware: bool = False)

Bases: [`Web3HTTPCommitmentService`](#vbase.Web3HTTPCommitmentService), `CommitmentServiceTest`

Test commitment service accessible using Web3.HTTPProvider.

#### add_object_with_timestamp(object_cid: str, timestamp: str) → dict

Test shim to record an object commitment with a given timestamp.
Only supported by test contracts.

* **Parameters:**
  * **object_cid** – The CID identifying the object.
  * **timestamp** – The timestamp to force for the record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_object_with_timestamp(set_cid: str, object_cid: str, timestamp: str) → dict

Test shim to record an object commitment with a given timestamp.
Only supported by test contracts.

* **Parameters:**
  * **set_cid** – The CID of the set containing the object.
  * **object_cid** – The CID to record.
  * **timestamp** – The timestamp to force for the record.
* **Returns:**
  The commitment log containing commitment receipt info.

#### add_set_objects_with_timestamps_batch(set_cid: str, object_cids: List[str], timestamps: List[str]) → List[dict]

Test shim to record a batch of object commitment with a timestamps.
Only supported by test contracts.

* **Parameters:**
  * **set_cid** – The CID of the set containing the objects.
  * **object_cids** – The hashes to record.
  * **timestamps** – The timestamps to force for the records.
* **Returns:**
  The commitment logs containing commitment receipts.

#### add_sets_objects_with_timestamps_batch(set_cids: List[str], object_cids: List[str], timestamps: List[str]) → List[dict]

Test shim to record a batch of object commitment with a timestamps.
Only supported by test contracts.

* **Parameters:**
  * **set_cids** – The hashes of the sets containing the objects.
  * **object_cids** – The hashes to record.
  * **timestamps** – The timestamps to force for the records.
* **Returns:**
  The commitment logs containing commitment receipts.

#### clear_set_objects(set_cid: str)

Clear all records (objects) for a user’s set.
Used to clear state when testing.
Only supported by test contracts.

* **Parameters:**
  **set_cid** – Hash identifying the set.

#### clear_sets()

Clear all sets for the user.
Used to clear state when testing.
Only supported by test contracts.

#### *static* create_instance_from_env(dotenv_path: str | None = None) → [Web3HTTPCommitmentServiceTest](#vbase.Web3HTTPCommitmentServiceTest)

Creates an instance initialized from environment variables.
Syntactic sugar for initializing new commitment objects using settings
stored in a .env file or in environment variables.

* **Parameters:**
  **dotenv_path** – 

  Path to the .env file.
  Below is the default treatment that should be appropriate in most scenarios:
  - If called with no arguments, or if the default None dotenv_path is specified,
  > default to the existing environment variables.
  - If dotenv_path is specified, attempt to load environment variables from the file.
* **Returns:**
  The dictionary of arguments.

### *class* vbase.Web3HTTPIndexingService(commitment_services: List[[Web3HTTPCommitmentService](#vbase.Web3HTTPCommitmentService)])

Bases: [`IndexingService`](#vbase.IndexingService)

Indexing service accessible using Web3.HTTPProvider.
Wraps RPC node event indexing to support commitment indexing operations.

#### *static* create_instance_from_env_json_descriptor(dotenv_path: str | None = None) → [Web3HTTPIndexingService](#vbase.Web3HTTPIndexingService)

Creates an instance initialized from an environment variable containing a JSON descriptor.
Syntactic sugar for initializing a new indexing service object using settings
stored in a .env file or in environment variables.
This method is especially useful for constructing complex
indexers using multiple commitment service defined using complex JSON.

* **Parameters:**
  **dotenv_path** – Path to the .env file.
  If path is not specified, does not load the .env file.
* **Returns:**
  The IndexingService created.

#### *static* create_instance_from_json_descriptor(is_json: str) → [Web3HTTPIndexingService](#vbase.Web3HTTPIndexingService)

Creates an instance initialized from a JSON descriptor.
This method is especially useful for constructing complex
indexers using multiple commitment service defined using complex JSON.

* **Parameters:**
  **is_json** – The JSON string with the initialization data.
* **Returns:**
  The IndexingService created.

#### find_last_object(object_cid: str, return_set_cid=False) → dict | None

Returns the last/latest receipt, if any, for object commitments.
Finds and returns individual object commitment irrespective of the set
it may have been committed to.

* **Parameters:**
  * **object_cid** – The CID for the object for search.
  * **return_set_cid** – If True, return the set CIDs, if any, for the object.
* **Returns:**
  The commitment receipt for the last/latest object commitment.

#### find_last_user_set_object(user: str, set_cid: str) → dict | None

Returns the last/latest receipt, if any, for user set object commitments
for a given user and set CID.

* **Parameters:**
  * **user** – The address for the user who made the commitment.
  * **set_cid** – The CID for the set containing the object.
* **Returns:**
  The commitment receipt for the last/latest user set commitment.

#### find_object(object_cid: str, return_set_cids=False) → List[dict]

Returns the list of receipts for object commitments
for a single object CID.
Finds and returns individual object commitments irrespective of the set
they may have been committed to.

* **Parameters:**
  * **object_cid** – The CID for the objects to search.
  * **return_set_cids** – If True, return the set CIDs, if any, for the objects.
* **Returns:**
  The list of commitment receipts for all object commitments.

#### find_objects(object_cids: List[str], return_set_cids=False) → List[dict]

Returns the list of receipts for object commitments
for a list of object CIDs.
Finds and returns individual object commitments irrespective of the set
they may have been committed to.

* **Parameters:**
  * **object_cids** – The CIDs for the objects to search.
  * **return_set_cids** – If True, return the set CIDs, if any, for the objects.
* **Returns:**
  The list of commitment receipts for all object commitments.

#### find_user_objects(user: str, return_set_cids=False) → List[dict]

Returns the list of receipts for user object commitments
for a given user.
Finds and returns individual object commitments irrespective of the set
they may have been committed to.

* **Parameters:**
  * **user** – The address for the user who made the commitments.
  * **object_cids** – The CIDs for the objects to search.
* **Returns:**
  The list of commitment receipts for all user object commitments.

#### find_user_set_objects(user: str, set_cid: str) → List[dict]

Returns the list of receipts for user set object commitments
for a given user and set CID.

* **Parameters:**
  * **user** – The address for the user who made the commitments.
  * **set_cid** – The CID for the set containing the objects.
* **Returns:**
  The list of commitment receipts for all user set object commitments.

#### find_user_sets(user: str) → List[dict]

Returns the list of receipts for user set commitments
for a given user.

* **Parameters:**
  **user** – The address for the user who made the commitments.
* **Returns:**
  The list of commitment receipts for all user set commitments.

### vbase.get_default_logger(name: str) → Logger

Get default logger for a given name.

* **Parameters:**
  **name** – The logger name.
* **Returns:**
  The logger object.
