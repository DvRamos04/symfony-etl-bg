<?php
namespace App\Service;

class Crypto {
  private string $key;
  private string $iv;

  public function __construct()
  {
    $this->key = base64_decode(getenv('CRYPTO_KEY_B64') ?: '');
    $this->iv  = base64_decode(getenv('CRYPTO_IV_B64')  ?: '');
    if (strlen($this->key)!==32 || strlen($this->iv)!==16) {
      throw new \RuntimeException('Invalid CRYPTO_KEY_B64/CRYPTO_IV_B64 length');
    }
  }

  public function encryptFile(string $in, string $out): void
  {
    $data = file_get_contents($in);
    $cipher = openssl_encrypt($data, 'AES-256-CBC', $this->key, OPENSSL_RAW_DATA, $this->iv);
    if ($cipher===false) { throw new \RuntimeException('Encrypt failed for '.$in); }
    file_put_contents($out, $cipher);
  }
}
