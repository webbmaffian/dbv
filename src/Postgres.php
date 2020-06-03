<?php

namespace Webbmaffian\DBV;

class Postgres extends DBV {
	protected function rename_table($old_name, $new_name) {
		$this->changes[] = $this->db->prepare('ALTER TABLE ' . $old_name . ' RENAME TO ' . $new_name);
	}


	protected function replace_function($function_name, $definition) {
		$function_name = $this->schema . '.' . $function_name;
		$definition = str_replace('FUNCTION ', 'FUNCTION ' . $this->schema . '.', $definition);

		parent::replace_function($function_name, $definition);
	}


	protected function change_column($column_name, $table_name, $fields = array(), $default = '', $extra = '') {
		$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' ALTER COLUMN ' . $column_name . ' TYPE ' . $fields['type'] . (!is_null($fields['max_len']) ? ' (' . $fields['max_len'] . ')': ''));
		$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' ALTER COLUMN ' . $column_name . ' ' . ($fields['null'] ? ' DROP NOT NULL' : ' SET NOT NULL'));
		
		if($default) {
			$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' ALTER COLUMN ' . $column_name . ' SET ' . $default);
		} else {
			$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' ALTER COLUMN ' . $column_name . ' DROP DEFAULT');
		}
	}


	protected function rename_column($old_name, $new_name, $table_name, $fields = array(), $default = '', $extra = '') {
		$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' RENAME COLUMN ' . $old_name . ' TO ' . $new_name);
	}


	protected function add_columns_to_query($new_columns, $table = null) {
		$columns = array();
		foreach($new_columns as $name => $fields) {
			$max_len = !is_null($fields['max_len']) ? ' (' . $fields['max_len'] . ') ': ' ';
			$default = $this->generate_default($fields['default']);
			$null = $fields['null'] ? '' : 'NOT NULL';

			$columns[] = $name . ' ' . $fields['type'] . $max_len . $null . $default;
		}

		return implode(', ', $columns);
	}


	protected function change_indexes($table, $old_indexes, $new_indexes) {
		if(isset($old_indexes)) {
			foreach(array_keys($old_indexes) as $index_name) {
				$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table . ' DROP CONSTRAINT IF EXISTS ' . $index_name);
				$this->changes[] = $this->db->prepare('DROP INDEX IF EXISTS ' . $index_name);
			}
		}

		foreach($new_indexes as $name => $definition) {
			$this->changes[] = $this->db->prepare($definition);
		}
	}


	protected function drop_foreign_keys($table, $foreign_keys) {
		// Not implemented yet
	}


	protected function add_foreign_keys($table, $foreign_keys) {
		// Not implemented yet
	}


	// Checks for auto increment sequences and prepares them for creation
	protected function generate_default($default) {
		if(is_null($default)) return null;
		$n_pos = strpos($default, 'nextval');
		if($n_pos !== false) {
			$offset = $n_pos + 9;
			$seq = trim(substr($default, $offset, strpos($default, '\'', $offset) - $offset), '"');
			$this->changes[] = $this->db->prepare('CREATE SEQUENCE IF NOT EXISTS ' . $seq);
		}
		return ' DEFAULT ' . $default;
	}


	protected function get_table_uuids() {
		$tables = array();
		$query = '
			SELECT relname AS name, obj_description(pg_class.oid) AS description
			FROM pg_class
			LEFT JOIN pg_catalog.pg_tables ON tablename = relname
			WHERE relkind = :relkind AND schemaname = :schema
			ORDER BY description
		';

		$result = $this->db->query($query, array(
			'relkind' => 'r',
			'schema' => $this->schema
		));

		while($row = $result->fetch_assoc()) {
			$tables[$row['name']] = $row['description'];
		}

		return $tables;
	}


	protected function get_table_columns($table) {
		$columns = array();
		$result = $this->db->query('
			SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
			FROM information_schema.columns
			WHERE table_name = :table AND table_schema = :schema
		', array(
			'table' => $table,
			'schema' => $this->schema
		));

		while($row = $result->fetch_assoc()) {
			if(strpos($row['column_default'], $this->schema) !== false) {
				$row['column_default'] = str_replace($this->schema . '.', '', $row['column_default']);
			}

			$columns[$row['column_name']] = array(
				'type' => $row['data_type'],
				'max_len' => $row['character_maximum_length'],
				'null' => $row['is_nullable'] === 'YES' ? true : false,
				'default' => $row['column_default']
			);
		}
		
		return $columns;
	}


	protected function get_table_indexes($table) {
		$indexes = array();
		$result = $this->db->query('
			SELECT relname as constraint_name, attname as column_name, indexdef, contype
			FROM pg_class
			LEFT JOIN pg_attribute ON attrelid = oid
			LEFT JOIN pg_indexes ON indexname = relname
			LEFT JOIN pg_constraint ON conname = relname
			WHERE tablename = :table AND schemaname = :schema
			GROUP BY relname, attname, indexdef, contype
			ORDER BY constraint_name
		', array(
			'table' => $table,
			'schema' => $this->schema
		));

		while($row = $result->fetch_assoc()) {

			// Skip indexes that start with an underscore
			if($row['constraint_name'][0] === '_') continue;

			// The contype is sometimes wrong - add an extra check so that the index
			// definition contains the UNIQUE word.
			if(!is_null($row['contype']) && strpos($row['indexdef'], 'UNIQUE') !== false) {

				// Index with constraint. Index will be created automatically when constraint is created.
				switch ($row['contype']) {
					case 'f':
						$type = 'FOREIGN KEY';
						break;
					case 'p':
						$type = 'PRIMARY KEY';
						break;
					case 'u':
						$type = 'UNIQUE';
						break;
					default:
						trigger_error('Constraint type not supported.', E_USER_WARNING);
						continue;
				}

				$cols = substr($row['indexdef'], strrpos($row['indexdef'], '('));
				$indexes[$row['constraint_name']] = 'ALTER TABLE ' . $table . ' ADD CONSTRAINT ' . $row['constraint_name'] . ' ' . $type . ' ' . $cols;
			}
			else {

				// Index with no constraint
				$indexes[$row['constraint_name']] = str_replace($this->schema . '.', '', $row['indexdef']);
			}

		}

		return $indexes;
	}


	// Not implemented yet
	protected function get_table_foreign_keys($table) {
		return [];
	}


	protected function get_database_functions() {
		$functions = array();
		$result = $this->db->query('
			SELECT 	p.proname as name,
					pg_get_functiondef(p.oid) as def
			FROM pg_catalog.pg_proc p
			JOIN pg_catalog.pg_roles u ON u.oid = p.proowner
			LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
			WHERE pg_catalog.pg_function_is_visible(p.oid) AND n.nspname = :schema AND u.rolname = current_user
		', array(
			'schema' => $this->schema
		));

		while($row = $result->fetch_assoc()) {
			$functions[$row['name']] = preg_replace('/FUNCTION [^\.\s]+\./', 'FUNCTION ', $row['def']);
		}

		return $functions;
	}


	protected function prepare_comment($table, $comment) {
		return $this->db->prepare('COMMENT ON TABLE ' . $this->schema . '.' . $table . ' IS ' . $this->db->escape_string($comment, true));
	}
}