<?php

namespace Webbmaffian\DBV;

use Webbmaffian\DBV\Helper\Uuid;

class DBV {

	protected $db = null;
	protected $schema;
	protected $changes = array();
	protected $drop_allowed = false;
	protected $ignore_primary_index = array();


	public function __construct($db, $schema, $drop_allowed = false) {
		$this->db = $db;
		$this->schema = $schema;
		$this->drop_allowed = $drop_allowed;
	}


	public function dump($filename) {
		return file_put_contents($filename, str_replace(str_repeat(' ', 4), "\t", json_encode($this->get_database_scheme(), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)));
	}


	public function test() {
		return $this->db->test();
	}


	public function start_transaction() {
		$this->db->start_transaction();
	}


	public function end_transaction() {
		$this->db->end_transaction();
	}


	public function rollback() {
		$this->db->rollback;
	}


	public function compare($new_db_filename) {
		$local_db = $this->get_database_scheme();
		$new_db = json_decode(file_get_contents($new_db_filename), true);
		$local_tables = $local_db['tables'];

		// Drop tables not in new version
		foreach($local_tables as $uuid => $old_table) {
			if(!isset($new_db['tables'][$uuid])) {
				if($this->drop_allowed) {
					trigger_error('DROPing table ' . $old_table['name'], E_USER_WARNING);
					$this->changes[] = $this->db->prepare('DROP TABLE ' . $old_table['name']);
				} else {
					throw new DBV_Exception('Table ' . $old_table['name'] . ' has been removed but DROP is not allowed.');
				}
			}
		}

		foreach($new_db['tables'] as $uuid => $new_table) {
			$has_changed = false;

			// Is table new or renamed?
			if(isset($local_tables[$uuid])) {
				if($new_table['name'] !== $local_tables[$uuid]['name']) {
					$this->changes[] = $this->db->prepare('ALTER TABLE ' . $local_tables[$uuid]['name'] . ' RENAME ' . $new_table['name']);
					$has_changed = true;
				}

				$has_changed = $this->check_columns($local_tables[$uuid], $new_table);
				
				// Has only indexes changed?
				if(isset($local_tables[$uuid])) {
					if(!empty(array_diff_assoc($new_table['indexes'], $local_tables[$uuid]['indexes'])) || count($new_table['indexes']) !== count($local_tables[$uuid]['indexes'])) {
						$has_changed = true;
					} 
				}
			} else {
				$this->create_table($uuid, $new_table);
				$has_changed = true;
			}

			// If a change has been made to a table or column indexes needs to be recreated
			if($has_changed) $this->change_indexes($new_table['name'], $local_tables[$uuid]['indexes'], $new_table['indexes']);
		}

		// Check functions
		$local_funcs = $local_db['functions'];
		foreach($new_db['functions'] as $name => $definition) {
			if(isset($local_funcs[$name]) && $local_funcs[$name] !== $definition) {
				$this->changes[] = $this->db->prepare('DROP FUNCTION ' . $name);
				$this->changes[] = $this->db->prepare($definition);
			} elseif(!isset($local_funcs[$name])) {
				$this->changes[] = $this->db->prepare($definition);
			}
		}

		return $this->changes;
	}


	protected function check_columns($old_table, $new_table) {
		$old_columns = $old_table['columns'];
		$new_columns = $new_table['columns'];
		$table_name = $new_table['name'];
		$has_changed = false;

		foreach($new_columns as $name => $fields) {
			$hash = md5(json_encode($fields));
			$default = $this->generate_default($fields['default']);
			$extra = (!empty($fields['extra']) ? (' ' . $fields['extra']) : '');

			if(isset($old_columns[$name])) {
				$old_hash = md5(json_encode($old_columns[$name]));

				if($hash === $old_hash) {
					continue;
				}

				// Column has changed
				$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' MODIFY ' . $name . ' ' . $fields['type'] . ($fields['null'] ? '' : ' NOT NULL') . ' ' . $default . $extra);
				$has_changed = true;
			} else {
				// Check if name has changed
				$renamed = false;
				$has_changed = true;
				foreach($old_columns as $old_name => $old_fields) {
					if(isset($new_columns[$old_name])) continue;
					
					$old_hash = md5(json_encode($old_fields));
					if($old_hash === $hash) {
						$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' CHANGE ' . $old_name . ' ' . $name . ' ' . $fields['type'] . ($fields['null'] ? '' : ' NOT NULL') . ' ' . $default . $extra);
						$renamed = true;
						break;
					}
				}

				// Add new column
				if($renamed) {
					$old_columns[$name] = $new_columns[$name];
					unset($old_columns[$old_name]);
				} else {
					$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' ADD COLUMN ' . $this->add_columns_to_query(array($name => $fields), $table_name));
				}
			}
		}

		// Drop deleted columns
		$new_keys = array_keys($new_columns);
		$old_keys = array_keys($old_columns);
		foreach(array_diff($old_keys, $new_keys) as $name) {
			if($this->drop_allowed) {
				trigger_error('DROPing column ' . $name, E_USER_WARNING);
				$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' DROP COLUMN ' . $name);
				$has_changed = true;
			}
		}

		return $has_changed;
	}


	protected function create_table($uuid, $table) {
		$query = 'CREATE TABLE ' . $table['name'] . ' (' . $this->add_columns_to_query($table['columns'], $table['name']) . ')';

		$this->changes[] = $this->db->prepare($query);
		$this->changes[] = $this->prepare_comment($table['name'], $uuid);

		return true;
	}


	protected function change_indexes($table, $old_indexes, $new_indexes) {
		foreach($new_indexes as $name => $definition) {
			if(isset($old_indexes[$name])) {
				
				// Indexes are identical - abort.
				if($old_indexes[$name] === $definition) {
					continue;
				}
				
				if($name === 'PRIMARY') {
					$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table . ' DROP PRIMARY KEY');
				}
				else {
					$this->changes[] = $this->db->prepare('DROP INDEX ' . $name . ' ON ' . $table);
				}
			}
			
			if($name === 'PRIMARY' && isset($this->ignore_primary_index[$table])) {
				continue;
			}
			
			$this->changes[] = $this->db->prepare($definition);
		}
	}


	protected function add_columns_to_query($new_columns, $table = null) {
		$columns = array();
		
		foreach($new_columns as $name => $fields) {
			$null = $fields['null'] ? '' : ' NOT NULL';
			
			if($fields['index'] === 'PRI' && $fields['extra'] === 'auto_increment') {
				$columns[] = $name . ' ' . $fields['type'] . $null . ' PRIMARY KEY AUTO_INCREMENT';
				
				// Ignore primary index if the column has AI - otherwise it will be added twice and result in a fatal error.
				if(!is_null($table)) {
					$this->ignore_primary_index[$table] = true;
				}

			} else {
				$columns[] = $name . ' ' . $fields['type'] . $null . ' ' . $this->generate_default($fields['default']);
			}
		}

		return implode(', ', $columns);
	}


	protected function get_database_scheme() {
		$this->check_table_uuid();

		return array(
			'tables' => $this->get_database_tables(),
			'functions' => $this->get_database_functions()
		);
	}


	protected function check_table_uuid() {
		$tables = $this->get_tables();

		foreach($tables as $name => $uuid) {
			if(empty($uuid)) {
				$this->prepare_comment($name, Uuid::get())->execute();
			}
		}
	}


	protected function get_database_tables() {
		$scheme = array();

		foreach($this->get_tables() as $table => $uuid) {
			$scheme[$uuid] = $this->get_table_scheme($table);
		}

		return $scheme;
	}


	protected function get_tables() {
		$tables = array();
		$query = '
			SELECT table_name AS name, table_comment AS comment
			FROM information_schema.tables
			WHERE table_schema = :schema
			ORDER BY table_comment
		';

		$result = $this->db->query($query, array('schema' => $this->schema));

		while($row = $result->fetch_assoc()) {
			$tables[$row['name']] = $row['comment'];
		}

		return $tables;
	}


	protected function get_table_scheme($table) {
		return array(
			'name' => $table,
			'columns' => $this->get_table_columns($table),
			'indexes' => $this->get_table_indexes($table)
		);
	}


	protected function get_table_columns($table) {
		$columns = array();
		$query = '
			SELECT column_name, column_type, is_nullable, column_default, column_key, extra
			FROM information_schema.columns
			WHERE table_name = :table AND table_schema = :schema
		';
		$result = $this->db->query($query, array('table' => $table, 'schema' => $this->schema));

		while($row = $result->fetch_assoc()) {
			$columns[$row['column_name']] = array(
				'type' => $row['column_type'],
				'null' => $row['is_nullable'] === 'YES' ? true : false,
				'default' => ($row['is_nullable'] === 'YES' && is_null($row['column_default'])) ? 'NULL' : $row['column_default'],
				'index' => $row['column_key'],
				'extra' => $row['extra']
			);
		}

		return $columns;
	}


	protected function get_table_indexes($table) {
		$temp_indexes = array();
		$indexes = array();
		$query = '
			SELECT index_name, column_name, non_unique, seq_in_index
			FROM information_schema.statistics
			WHERE table_name = :table AND table_schema = :schema
		';

		$result = $this->db->query($query, array('table' => $table, 'schema' => $this->schema));

		while($row = $result->fetch_assoc()) {
			if(!isset($temp_indexes[$row['index_name']])) {
				if($row['index_name'] === 'PRIMARY') {
					$type = 'PRIMARY';
				} elseif(!$row['non_unique']) {
					$type = 'UNIQUE INDEX';
				} else {
					$type = 'INDEX';
				}

				$temp_indexes[$row['index_name']] = array(
					'type' => $type,
					'columns' => array()
				);
			}

			$temp_indexes[$row['index_name']]['columns'][$row['seq_in_index']] = $row['column_name'];
		}

		foreach($temp_indexes as $name => $values) {
			if($values['type'] === 'PRIMARY') {
				$def = 'ALTER TABLE ' . $table . ' ADD PRIMARY KEY (' . implode(', ', $values['columns']) . ')';
			} else {
				$def = 'ALTER TABLE ' . $table . ' ADD ' . $values['type'] . ' ' . $name . ' (' . implode(', ', $values['columns']) . ')';
			}

			$indexes[$name] = $def;
		}

		return $indexes;
	}


	protected function get_database_functions() {
		$functions = array();
		
		return $functions;
		
		$query = 'SELECT routine_name AS name, routine_definition AS definition FROM information_schema.routines';
		$result = $this->db->query($query);

		while($row = $result->fetch_assoc()) {
			$functions[$row['name']] = $row['definition'];
		}

		return $functions;
	}


	protected function prepare_comment($table, $comment) {
		$stmt = $this->db->prepare('ALTER TABLE ' . $table . ' COMMENT "' . $comment . '"');
		return $stmt;
	}


	private function generate_default($default) {
		return !empty($default) ? 'DEFAULT ' . $default : '';
	}
}