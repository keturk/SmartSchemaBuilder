CREATE SCHEMA IF NOT EXISTS medical;

CREATE TABLE IF NOT EXISTS medical.clients (
	client_id bigint NOT NULL,
	name text NOT NULL,
	address text ,
	phone text NOT NULL,
	PRIMARY KEY (client_id),
	CONSTRAINT idx_clients_namee1c76 UNIQUE(name),
	CONSTRAINT idx_clients_phonef9a4d UNIQUE(phone)
);


CREATE TABLE IF NOT EXISTS medical.staff (
	staff_id bigint NOT NULL,
	name text NOT NULL,
	position text ,
	PRIMARY KEY (staff_id),
	CONSTRAINT idx_staff_name07aa2 UNIQUE(name)
);


CREATE TABLE IF NOT EXISTS medical.test_data_items (
	item_id bigint NOT NULL,
	name text NOT NULL,
	description text ,
	PRIMARY KEY (item_id),
	CONSTRAINT idx_test_data_items_name0c833 UNIQUE(name)
);


CREATE TABLE IF NOT EXISTS medical.test_types (
	type_id bigint NOT NULL,
	name text NOT NULL,
	description text ,
	PRIMARY KEY (type_id),
	CONSTRAINT idx_test_types_nameb83e7 UNIQUE(name)
);


CREATE TABLE IF NOT EXISTS medical.test_definitions (
	definition_id bigint NOT NULL,
	type_id bigint ,
	name text NOT NULL,
	description text ,
	PRIMARY KEY (definition_id),
	CONSTRAINT fk_test_definitions_type_id_test_types_type_id6166f FOREIGN KEY (type_id)
		REFERENCES medical.test_types (type_id) ON DELETE CASCADE,
	CONSTRAINT idx_test_definitions_nameccdfb UNIQUE(name)
);


CREATE TABLE IF NOT EXISTS medical.data_items_in_tests (
	definition_id bigint NOT NULL,
	item_id bigint NOT NULL,
	CONSTRAINT fk_data_items_in_tests_definition_id_test_definitions_defie9818 FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id) ON DELETE CASCADE,

	CONSTRAINT fk_data_items_in_tests_item_id_test_data_items_item_id2145b FOREIGN KEY (item_id)
		REFERENCES medical.test_data_items (item_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS medical.tests_administered (
	administered_id bigint NOT NULL,
	definition_id bigint ,
	client_id bigint ,
	staff_id bigint ,
	date_administered text NOT NULL,
	PRIMARY KEY (administered_id),
	CONSTRAINT fk_tests_administered_definition_id_test_definitions_defin91abc FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id) ON DELETE CASCADE,

	CONSTRAINT fk_tests_administered_client_id_clients_client_idc5895 FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id) ON DELETE CASCADE,

	CONSTRAINT fk_tests_administered_staff_id_staff_staff_idce8fb FOREIGN KEY (staff_id)
		REFERENCES medical.staff (staff_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS medical.test_results (
	result_id bigint NOT NULL,
	administered_id bigint ,
	value double precision ,
	notes text ,
	PRIMARY KEY (result_id),
	CONSTRAINT fk_test_results_administered_id_tests_administered_adminis5c687 FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS medical.payments (
	payment_id bigint NOT NULL,
	administered_id bigint ,
	client_id bigint ,
	payment_date text NOT NULL,
	amount double precision ,
	PRIMARY KEY (payment_id),
	CONSTRAINT fk_payments_administered_id_tests_administered_administered7488 FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id) ON DELETE CASCADE,

	CONSTRAINT fk_payments_client_id_clients_client_id70268 FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id) ON DELETE CASCADE
);


