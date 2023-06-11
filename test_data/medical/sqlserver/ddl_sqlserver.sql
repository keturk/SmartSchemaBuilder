IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'medical')
BEGIN
   EXEC('CREATE SCHEMA medical')
END;

CREATE TABLE medical.clients (
	client_id bigint ,
	name text ,
	address text ,
	phone text ,
	CONSTRAINT PK_clients PRIMARY KEY (client_id)
);


CREATE TABLE medical.staff (
	staff_id bigint ,
	name text ,
	position text ,
	CONSTRAINT PK_staff PRIMARY KEY (staff_id)
);


CREATE TABLE medical.test_data_items (
	item_id bigint ,
	name text ,
	description text ,
	CONSTRAINT PK_test_data_items PRIMARY KEY (item_id)
);


CREATE TABLE medical.test_types (
	type_id bigint ,
	name text ,
	description text ,
	CONSTRAINT PK_test_types PRIMARY KEY (type_id)
);


CREATE TABLE medical.test_definitions (
	definition_id bigint ,
	type_id bigint ,
	name text ,
	description text ,
	CONSTRAINT PK_test_definitions PRIMARY KEY (definition_id),
	CONSTRAINT fk_test_definitions_type_id_test_types_type_id FOREIGN KEY (type_id)
		REFERENCES medical.test_types (type_id)
);


CREATE TABLE medical.tests_administered (
	administered_id bigint ,
	definition_id bigint ,
	client_id bigint ,
	staff_id bigint ,
	date_administered text ,
	CONSTRAINT PK_tests_administered PRIMARY KEY (administered_id),
	CONSTRAINT fk_tests_administered_definition_id_test_definitions_definition_id FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id),

	CONSTRAINT fk_tests_administered_client_id_clients_client_id FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id),

	CONSTRAINT fk_tests_administered_staff_id_staff_staff_id FOREIGN KEY (staff_id)
		REFERENCES medical.staff (staff_id)
);


CREATE TABLE medical.data_items_in_tests (
	definition_id bigint ,
	item_id bigint ,
	CONSTRAINT PK_data_items_in_tests PRIMARY KEY (definition_id,item_id),
	CONSTRAINT fk_data_items_in_tests_definition_id_test_definitions_definition_id FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id),

	CONSTRAINT fk_data_items_in_tests_item_id_test_data_items_item_id FOREIGN KEY (item_id)
		REFERENCES medical.test_data_items (item_id)
);


CREATE TABLE medical.payments (
	payment_id bigint ,
	administered_id bigint ,
	client_id bigint ,
	amount double precision ,
	payment_date text ,
	CONSTRAINT PK_payments PRIMARY KEY (payment_id),
	CONSTRAINT fk_payments_administered_id_tests_administered_administered_id FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id),

	CONSTRAINT fk_payments_client_id_clients_client_id FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id)
);


CREATE TABLE medical.test_results (
	result_id bigint ,
	administered_id bigint ,
	value double precision ,
	notes text ,
	CONSTRAINT PK_test_results PRIMARY KEY (result_id),
	CONSTRAINT fk_test_results_administered_id_tests_administered_administered_id FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id)
);


