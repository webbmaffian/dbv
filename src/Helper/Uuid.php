<?php

/*
	Credit to https://github.com/oittaa/uuid-php/blob/master/uuid.php
*/

namespace Webbmaffian\DBV\Helper;

class Uuid {
	private static function uuid_from_hash($hash, $version) {
        return sprintf(
					'%08s-%04s-%04x-%04x-%12s',
         		  	substr($hash, 0, 8), // 32 bits for "time_low"
            		substr($hash, 8, 4), // 16 bits for "time_mid"
            		(hexdec(substr($hash, 12, 4)) & 0x0fff) | $version << 12, // 16 bits for "time_hi_and_version", four most significant bits holds version number
		            (hexdec(substr($hash, 16, 4)) & 0x3fff) | 0x8000, // 16 bits, 8 bits for "clk_seq_hi_res", 8 bits for "clk_seq_low", two most significant bits holds zero and one for variant DCE1.1
					substr($hash, 20, 12) // 48 bits for "node"
				);
	}


	public static function is_valid($uuid) {
        return preg_match('/^(urn:)?(uuid:)?(\{)?[0-9a-f]{8}\-?[0-9a-f]{4}\-?[0-9a-f]{4}\-?[0-9a-f]{4}\-?[0-9a-f]{12}(?(3)\}|)$/i', $uuid) === 1;
	}
	

	public static function get() {
        $bytes = function_exists('random_bytes') ? random_bytes(16) : openssl_random_pseudo_bytes(16);
        $hash = bin2hex($bytes);
        return self::uuid_from_hash($hash, 4);
    }
}