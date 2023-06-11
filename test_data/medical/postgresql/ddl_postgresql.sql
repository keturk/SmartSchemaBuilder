CREATE SCHEMA IF NOT EXISTS medical;

CREATE TABLE IF NOT EXISTS medical.clients (
	client_id bigint ,
	name text ,
	address text ,
	phone text ,
	PRIMARY KEY (client_id)
);


CREATE TABLE IF NOT EXISTS medical.staff (
	staff_id bigint ,
	name text ,
	position text ,
	PRIMARY KEY (staff_id)
);


CREATE TABLE IF NOT EXISTS medical.test_data_items (
	item_id bigint ,
	name text ,
	description text ,
	PRIMARY KEY (item_id)
);


CREATE TABLE IF NOT EXISTS medical.test_types (
	type_id bigint ,
	name text ,
	description text ,
	PRIMARY KEY (type_id)
);


CREATE TABLE IF NOT EXISTS medical.test_definitions (
	definition_id bigint ,
	type_id bigint ,
	name text ,
	description text ,
	PRIMARY KEY (definition_id),
	CONSTRAINT fk_test_definitions_type_id_test_types_type_id FOREIGN KEY (type_id)
		REFERENCES medical.test_types (type_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS medical.data_items_in_tests (
	definition_id bigint ,
	item_id bigint ,
	PRIMARY KEY (definition_id,item_id),
	CONSTRAINT fk_data_items_in_tests_definition_id_test_definitions_defie9818 FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id) ON DELETE CASCADE,

	CONSTRAINT fk_data_items_in_tests_item_id_test_data_items_item_id FOREIGN KEY (item_id)
		REFERENCES medical.test_data_items (item_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS medical.tests_administered (
	administered_id bigint ,
	definition_id bigint ,
	client_id bigint ,
	staff_id bigint ,
	date_administered text ,
	PRIMARY KEY (administered_id),
	CONSTRAINT fk_tests_administered_definition_id_test_definitions_defin91abc FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id) ON DELETE CASCADE,

	CONSTRAINT fk_tests_administered_client_id_clients_client_id FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id) ON DELETE CASCADE,

	CONSTRAINT fk_tests_administered_staff_id_staff_staff_id FOREIGN KEY (staff_id)
		REFERENCES medical.staff (staff_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS medical.test_results (
	result_id bigint ,
	administered_id bigint ,
	value double precision ,
	notes text ,
	PRIMARY KEY (result_id),
	CONSTRAINT fk_test_results_administered_id_tests_administered_adminis5c687 FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS medical.payments (
	payment_id bigint ,
	administered_id bigint ,
	client_id bigint ,
	amount double precision ,
	payment_date text ,
	PRIMARY KEY (payment_id),
	CONSTRAINT fk_payments_administered_id_tests_administered_administered7488 FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id) ON DELETE CASCADE,

	CONSTRAINT fk_payments_client_id_clients_client_id FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id) ON DELETE CASCADE
);


