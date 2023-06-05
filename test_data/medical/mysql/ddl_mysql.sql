CREATE SCHEMA IF NOT EXISTS medical;

CREATE TABLE IF NOT EXISTS medical.clients (
	client_id BIGINT NOT NULL,
	name VARCHAR(255) NOT NULL,
	address VARCHAR(255) ,
	phone VARCHAR(255) NOT NULL,
	PRIMARY KEY (client_id),
	UNIQUE INDEX idx_clients_namee1c76 (name),
	UNIQUE INDEX idx_clients_phonef9a4d (phone)
);



CREATE TABLE IF NOT EXISTS medical.staff (
	staff_id BIGINT NOT NULL,
	name VARCHAR(255) NOT NULL,
	position VARCHAR(255) ,
	PRIMARY KEY (staff_id),
	UNIQUE INDEX idx_staff_name07aa2 (name)
);



CREATE TABLE IF NOT EXISTS medical.test_data_items (
	item_id BIGINT NOT NULL,
	name VARCHAR(255) NOT NULL,
	description VARCHAR(255) ,
	PRIMARY KEY (item_id),
	UNIQUE INDEX idx_test_data_items_name0c833 (name)
);



CREATE TABLE IF NOT EXISTS medical.test_types (
	type_id BIGINT NOT NULL,
	name VARCHAR(255) NOT NULL,
	description VARCHAR(255) ,
	PRIMARY KEY (type_id),
	UNIQUE INDEX idx_test_types_nameb83e7 (name)
);



CREATE TABLE IF NOT EXISTS medical.test_definitions (
	definition_id BIGINT NOT NULL,
	type_id BIGINT ,
	name VARCHAR(255) NOT NULL,
	description VARCHAR(255) ,
	PRIMARY KEY (definition_id),
	CONSTRAINT fk_test_definitions_type_id_test_types_type_id6166f FOREIGN KEY (type_id)
		REFERENCES medical.test_types (type_id) ON DELETE CASCADE,
	UNIQUE INDEX idx_test_definitions_nameccdfb (name)
);



CREATE TABLE IF NOT EXISTS medical.tests_administered (
	administered_id BIGINT NOT NULL,
	definition_id BIGINT ,
	client_id BIGINT ,
	staff_id BIGINT ,
	date_administered VARCHAR(255) NOT NULL,
	PRIMARY KEY (administered_id),
	CONSTRAINT fk_tests_administered_definition_id_test_definitions_defini91abc FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id) ON DELETE CASCADE,

	CONSTRAINT fk_tests_administered_client_id_clients_client_idc5895 FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id) ON DELETE CASCADE,

	CONSTRAINT fk_tests_administered_staff_id_staff_staff_idce8fb FOREIGN KEY (staff_id)
		REFERENCES medical.staff (staff_id) ON DELETE CASCADE
);



CREATE TABLE IF NOT EXISTS medical.data_items_in_tests (
	definition_id BIGINT NOT NULL,
	item_id BIGINT NOT NULL,
	CONSTRAINT fk_data_items_in_tests_definition_id_test_definitions_define9818 FOREIGN KEY (definition_id)
		REFERENCES medical.test_definitions (definition_id) ON DELETE CASCADE,

	CONSTRAINT fk_data_items_in_tests_item_id_test_data_items_item_id2145b FOREIGN KEY (item_id)
		REFERENCES medical.test_data_items (item_id) ON DELETE CASCADE
);



CREATE TABLE IF NOT EXISTS medical.payments (
	payment_id BIGINT NOT NULL,
	administered_id BIGINT ,
	client_id BIGINT ,
	payment_date VARCHAR(255) NOT NULL,
	amount DOUBLE ,
	PRIMARY KEY (payment_id),
	CONSTRAINT fk_payments_administered_id_tests_administered_administeredd7488 FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id) ON DELETE CASCADE,

	CONSTRAINT fk_payments_client_id_clients_client_id70268 FOREIGN KEY (client_id)
		REFERENCES medical.clients (client_id) ON DELETE CASCADE
);



CREATE TABLE IF NOT EXISTS medical.test_results (
	result_id BIGINT NOT NULL,
	administered_id BIGINT ,
	value DOUBLE ,
	notes VARCHAR(255) ,
	PRIMARY KEY (result_id),
	CONSTRAINT fk_test_results_administered_id_tests_administered_administ5c687 FOREIGN KEY (administered_id)
		REFERENCES medical.tests_administered (administered_id) ON DELETE CASCADE
);



