<?php

namespace Webbmaffian\DBV;

class Mysql extends DBV {
	private $ignore_primary_index = array();


	protected function rename_table($old_name, $new_name) {
		$this->changes[] = $this->db->prepare('ALTER TABLE ' . $old_name . ' RENAME ' . $new_name);
	}


	protected function change_column($column_name, $table_name, $fields = array(), $default = '', $extra = '') {
		$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' MODIFY ' . $column_name . ' ' . $fields['type'] . ($fields['null'] ? '' : ' NOT NULL') . ' ' . $default . $extra);
	}


	protected function rename_column($old_name, $new_name, $table_name, $fields = array(), $default = '', $extra = '') {
		$this->changes[] = $this->db->prepare('ALTER TABLE ' . $table_name . ' CHANGE ' . $old_name . ' ' . $new_name . ' ' . $fields['type'] . ($fields['null'] ? '' : ' NOT NULL') . ' ' . $default . $extra);
	}


	protected function create_table(string $uuid, array $table) {
		$query = 'CREATE TABLE ' . $table['name'] . ' (' . $this->add_columns_to_query($table['columns'], $table['name']) . ')';

		if($this->collation) {
			$query .= sprintf(' CHARACTER SET %s COLLATE %s', ...$this->collation);
		}

		$this->changes[] = $this->db->prepare($query);
		$this->changes[] = $this->prepare_comment($table['name'], $uuid);

		return true;
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


	protected function drop_foreign_keys($table, $foreign_keys) {
		foreach(array_keys($foreign_keys) as $name) {
			$this->changes[] = $this->db->prepare(sprintf('ALTER TABLE %s DROP FOREIGN KEY %s', $table, $name));
		}
	}


	protected function add_foreign_keys($table, $foreign_keys) {
		foreach($foreign_keys as $definition) {
			$this->changes[] = $this->db->prepare($definition);
		}
	}


	protected function generate_default($default) {
		return (!is_null($default) ? 'DEFAULT ' . $default : '');
	}


	protected function get_table_uuids() {
		$tables = array();
		$query = '
			SELECT table_name AS name, table_comment AS comment
			FROM information_schema.tables
			WHERE table_schema = :schema
			ORDER BY table_comment
		';

		$result = $this->db->query($query, array('schema' => $this->schema));

		while($row = $result->fetch_assoc()) {
			$row = array_change_key_case($row);

			$tables[$row['name']] = $row['comment'];
		}

		return $tables;
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
			$row = array_change_key_case($row);

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
			ORDER BY index_name
		';

		$result = $this->db->query($query, array('table' => $table, 'schema' => $this->schema));

		while($row = $result->fetch_assoc()) {
			$row = array_change_key_case($row);
			
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
			ksort($values['columns'], SORT_NUMERIC);
			
			if($values['type'] === 'PRIMARY') {
				$def = 'ALTER TABLE ' . $table . ' ADD PRIMARY KEY (' . implode(', ', $values['columns']) . ')';
			} else {
				$def = 'ALTER TABLE ' . $table . ' ADD ' . $values['type'] . ' ' . $name . ' (' . implode(', ', $values['columns']) . ')';
			}

			$indexes[$name] = $def;
		}

		return $indexes;
	}


	// We only support one-column foreign keys right now
	protected function get_table_foreign_keys($table) {
		$query = '
			SELECT
				referential_constraints.constraint_name,
				key_column_usage.column_name,
				key_column_usage.referenced_table_name,
				key_column_usage.referenced_column_name,
				referential_constraints.update_rule,
				referential_constraints.delete_rule

			FROM information_schema.referential_constraints

			LEFT JOIN information_schema.key_column_usage USING (constraint_schema, constraint_name)

			WHERE referential_constraints.constraint_schema = :schema
			AND key_column_usage.table_name = :table
			AND key_column_usage.referenced_table_schema = key_column_usage.constraint_schema

			GROUP BY referential_constraints.constraint_name

			ORDER BY constraint_name
		';

		$result = $this->db->query($query, [
			'table' => $table,
			'schema' => $this->schema
		]);

		$foreign_keys = [];

		while($row = $result->fetch_assoc()) {
			$row = array_change_key_case($row);

			$foreign_keys[$row['constraint_name']] = sprintf(
				'ALTER TABLE %s ADD FOREIGN KEY %s (%s) REFERENCES %s (%s) ON DELETE %s ON UPDATE %s',
				$table,
				$row['constraint_name'],
				$row['column_name'],
				$row['referenced_table_name'],
				$row['referenced_column_name'],
				$row['delete_rule'],
				$row['update_rule']
			);
		}

		return $foreign_keys;
	}


	// Something weird about this function, thus inactive (not needed yet anyway)
	/*protected function get_database_functions() {
		$functions = array();
		
		$query = 'SELECT routine_name AS name, routine_definition AS definition FROM information_schema.routines';
		$result = $this->db->query($query);

		while($row = $result->fetch_assoc()) {
			$functions[$row['name']] = $row['definition'];
		}

		return $functions;
	}*/


	protected function prepare_comment($table, $comment) {
		return $this->db->prepare('ALTER TABLE ' . $table . ' COMMENT ' . $this->db->escape_string($comment, true));
	}
}
