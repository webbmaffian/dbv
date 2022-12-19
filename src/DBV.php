<?php

namespace Webbmaffian\DBV;

use Webbmaffian\DBV\Helper\DBV_Exception;
use Webbmaffian\ORM\Interfaces\Database;
use Webbmaffian\DBV\Helper\Uuid;

abstract class DBV {
	protected $db = null;
	protected $schema;
	protected $changes = array();
	protected $drop_allowed = false;
	protected $collation = null;


	abstract protected function change_indexes($table, $old_indexes, $new_indexes);
	abstract protected function rename_table($old_name, $new_name);
	abstract protected function create_table(string $uuid, array $table);
	abstract protected function change_column($column_name, $table_name, $fields = array(), $default = '', $extra = '');
	abstract protected function rename_column($old_name, $new_name, $table_name, $fields = array(), $default = '', $extra = '');
	abstract protected function add_columns_to_query($new_columns, $table = null);
	abstract protected function generate_default($default);
	abstract protected function prepare_comment($table, $comment);
	abstract protected function get_table_uuids();
	abstract protected function get_table_columns($table);
	abstract protected function get_table_indexes($table);
	abstract protected function get_table_foreign_keys($table);
	abstract protected function drop_foreign_keys($table, $foreign_keys);
	abstract protected function add_foreign_keys($table, $foreign_keys);


	// This function returns a new instance of the correct DBV class
	static public function instance($db, $drop_allowed = false) {
		if(!$db instanceof Database) {
			throw new DBV_Exception('Database instance does not implement the Database interface.');
		}

		$class_name = substr(strrchr(get_class($db), '\\'), 1);
		$full_class_name = __NAMESPACE__ . '\\' . $class_name;

		if(!class_exists($full_class_name)) {
			throw new DBV_Exception('No DBV class exists for ' . $class_name);
		}

		return new $full_class_name($db, $drop_allowed);
	}


	public function __construct($db, $drop_allowed = false) {
		if(!$db instanceof Database) {
			throw new DBV_Exception('Database instance does not implement the Database interface.');
		}

		$this->db = $db;
		$this->schema = $db->get_schema();
		$this->drop_allowed = $drop_allowed;
	}


	public function set_collation($collation, $charset = null) {
		if(!preg_match('/^[a-z0-9_]+$/', $collation)) {
			throw new DBV_Exception('Invalid collation.');
		}

		if(is_null($charset)) {
			$charset = strtok($collation, '_');
		}
		elseif(!preg_match('/^[a-z0-9_]+$/', $charset)) {
			throw new DBV_Exception('Invalid collation.');
		}

		$this->collation = [$charset, $collation];
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
		$this->db->rollback();
	}


	public function compare($new_db_filename) {
		$this->changes = array();

		$local_db = $this->get_database_scheme();
		$new_db = $this->get_dumped_scheme($new_db_filename);
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
			$old_indexes = null;
			$old_foreign_keys = [];

			// Backwards compatibility
			if(!isset($new_table['foreign_keys'])) {
				$new_table['foreign_keys'] = [];
			}

			// Is table new or renamed?
			if(isset($local_tables[$uuid])) {
				if($new_table['name'] !== $local_tables[$uuid]['name']) {
					$this->rename_table($local_tables[$uuid]['name'], $new_table['name']);
					$has_changed = true;
				}

				$has_changed = $this->check_columns($local_tables[$uuid], $new_table);
				$old_indexes = $local_tables[$uuid]['indexes'];
				$old_foreign_keys = $local_tables[$uuid]['foreign_keys'] ?? [];
				
				// Has only indexes and/or foreign keys changed?
				if(isset($local_tables[$uuid])) {
					if(!empty(array_diff_assoc($new_table['indexes'], $old_indexes)) || (count($new_table['indexes']) !== count($old_indexes))) {
						$has_changed = true;
					}
					elseif(!empty(array_diff_assoc($new_table['foreign_keys'], $old_foreign_keys)) || (count($new_table['foreign_keys']) !== count($old_foreign_keys))) {
						$has_changed = true;
					}
				}
			} else {
				$this->create_table($uuid, $new_table);
				$has_changed = true;
			}

			// If a change has been made to a table or column indexes needs to be recreated
			if($has_changed) {
				$this->drop_foreign_keys($new_table['name'], $old_foreign_keys);
				$this->change_indexes($new_table['name'], $old_indexes, $new_table['indexes']);
				$this->add_foreign_keys($new_table['name'], $new_table['foreign_keys']);
			}
		}

		// Check functions
		$local_funcs = $local_db['functions'];
		foreach($new_db['functions'] as $name => $definition) {
			if(!isset($local_funcs[$name]) || $local_funcs[$name] !== $definition) {
				$this->replace_function($name, $definition);
			}
		}

		return $this->changes;
	}


	protected function replace_function($function_name, $definition) {
		$this->changes[] = $this->db->prepare('DROP FUNCTION IF EXISTS ' . $function_name);
		$this->changes[] = $this->db->prepare($definition);
	}


	protected function check_columns(array $old_table, array $new_table) {
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
				$this->change_column($name, $table_name, $fields, $default, $extra);
				$has_changed = true;
			} else {
				// Check if name has changed
				$renamed = false;
				$has_changed = true;
				foreach($old_columns as $old_name => $old_fields) {
					if(isset($new_columns[$old_name])) continue;
					
					$old_hash = md5(json_encode($old_fields));
					if($old_hash === $hash) {
						$this->rename_column($old_name, $name, $table_name, $fields, $default, $extra);
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
		if($this->drop_allowed) {
			$new_keys = array_keys($new_columns);
			$old_keys = array_keys($old_columns);

			foreach(array_diff($old_keys, $new_keys) as $name) {
				if(empty($name)) continue;
				
				trigger_error('DROPing column ' . $name, E_USER_WARNING);
				$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' DROP COLUMN ' . $name);
				$has_changed = true;
			}
		}

		return $has_changed;
	}


	protected function check_table_uuid() {
		$tables = $this->get_table_uuids();

		foreach($tables as $name => $uuid) {
			if(empty($uuid)) {
				$this->prepare_comment($name, Uuid::get())->execute();
			}
		}
	}


	protected function get_database_scheme() {
		$this->check_table_uuid();

		return array(
			'tables' => $this->get_database_tables(),
			'functions' => $this->get_database_functions()
		);
	}


	protected function get_dumped_scheme($filepath) {
		if(!file_exists($filepath)) {
			throw new DBV_Exception('Provided dump file does not exist.');
		}

		return json_decode(file_get_contents($filepath), true);
	}


	protected function get_database_tables() {
		$scheme = array();

		foreach($this->get_table_uuids() as $table => $uuid) {
			$scheme[$uuid] = $this->get_table_scheme($table);
		}

		return $scheme;
	}


	protected function get_table_scheme($table) {
		return array(
			'name' => $table,
			'columns' => $this->get_table_columns($table),
			'indexes' => $this->get_table_indexes($table),
			'foreign_keys' => $this->get_table_foreign_keys($table)
		);
	}


	protected function get_database_functions() {
		return array();
	}


	public function repair_uuids($filepath) {
		$this->changes = array();

		$scheme = $this->get_dumped_scheme($filepath);
		$tables = $this->get_table_uuids();

		foreach($scheme['tables'] as $uuid => $table) {
			if(!isset($tables[$table['name']])) {
				continue;
			}

			if($tables[$table['name']] !== $uuid) {
				$this->changes[] = $this->prepare_comment($table['name'], $uuid);
			}
		}

		return $this->changes;
	}
}
