IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'medical')
BEGIN
   EXEC('CREATE SCHEMA medical')
END;

CREATE TABLE medical.clients (
	client_id BIGINT NOT NULL,
	name NVARCHAR(255) NOT NULL,
	address NVARCHAR(255) ,
	phone NVARCHAR(255) NOT NULL,
	CONSTRAINT PK_clients PRIMARY KEY (client_id),
	CONSTRAINT idx_clients_namee1c76 UNIQUE (name),
	CONSTRAINT idx_clients_phonef9a4d UNIQUE (phone)
);


CREATE TABLE medical.staff (
	staff_id BIGINT NOT NULL,
	name NVARCHAR(255) NOT NULL,
	position NVARCHAR(255) ,
	CONSTRAINT PK_staff PRIMARY KEY (staff_id),
	CONSTRAINT idx_staff_name07aa2 UNIQUE (name)
);


CREATE TABLE medical.test_data_items (
	item_id BIGINT NOT NULL,
	name NVARCHAR(255) NOT NULL,
	description NVARCHAR(255) ,
	CONSTRAINT PK_test_data_items PRIMARY KEY (item_id),
	CONSTRAINT idx_test_data_items_name0c833 UNIQUE (name)
);


CREATE TABLE medical.test_types (
	type_id BIGINT NOT NULL,
	name NVARCHAR(255) NOT NULL,
	description NVARCHAR(255) ,
	CONSTRAINT PK_test_types PRIMARY KEY (type_id),
	CONSTRAINT idx_test_types_nameb83e7 UNIQUE (name)
);


CREATE TABLE medical.test_definitions (
	definition_id BIGINT NOT NULL,
	type_id BIGINT ,
	name NVARCHAR(255) NOT NULL,
	description NVARCHAR(255) ,
	CONSTRAINT PK_test_definitions PRIMARY KEY (definition_id),
	CONSTRAINT fk_test_definitions_type_id_test_types_type_id6166f FOREIGN KEY (type_id)
		REFERENCES medical.test_types (type_id),
	CONSTRAINT idx_test_definitions_nameccdfb UNIQUE (name)
);


CREATE TABLE medical.data_items_in_tests (
	definition_id BIGINT NOT NULL,
	item_id BIGINT NOT NULL,
	CONSTRAINT fk_data_items_in_tests_definition_id_test_definitions_definition_ide9818 FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id),

	CONSTRAINT fk_data_items_in_tests_item_id_test_data_items_item_id2145b FOREIGN KEY (item_id)
		REFERENCES medical.test_data_items (item_id)
);


CREATE TABLE medical.tests_administered (
	administered_id BIGINT NOT NULL,
	definition_id BIGINT ,
	client_id BIGINT ,
	staff_id BIGINT ,
	date_administered NVARCHAR(255) NOT NULL,
	CONSTRAINT PK_tests_administered PRIMARY KEY (administered_id),
	CONSTRAINT fk_tests_administered_definition_id_test_definitions_definition_id91abc FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id),

	CONSTRAINT fk_tests_administered_client_id_clients_client_idc5895 FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id),

	CONSTRAINT fk_tests_administered_staff_id_staff_staff_idce8fb FOREIGN KEY (staff_id)
		REFERENCES medical.staff (staff_id)
);


CREATE TABLE medical.payments (
	payment_id BIGINT NOT NULL,
	administered_id BIGINT ,
	client_id BIGINT ,
	payment_date NVARCHAR(255) NOT NULL,
	amount FLOAT ,
	CONSTRAINT PK_payments PRIMARY KEY (payment_id),
	CONSTRAINT fk_payments_administered_id_tests_administered_administered_idd7488 FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id),

	CONSTRAINT fk_payments_client_id_clients_client_id70268 FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id)
);


CREATE TABLE medical.test_results (
	result_id BIGINT NOT NULL,
	administered_id BIGINT ,
	value FLOAT ,
	notes NVARCHAR(255) ,
	CONSTRAINT PK_test_results PRIMARY KEY (result_id),
	CONSTRAINT fk_test_results_administered_id_tests_administered_administered_id5c687 FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id)
);


