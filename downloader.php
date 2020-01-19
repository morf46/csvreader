<?php


class FileDownloader
{
    private function StartFileStream($Origin, $Destination)
    {
        /**
         * Just accept the certificate.
         */
        $context = stream_context_create(array(
            'ssl' => array(
                'verify_peer' => false,
                'verify_peer_name' => false
            )
        ));



        $rh = fopen($Origin, 'rb', false, $context);
        $wh = fopen($Destination, 'w+b');
        if (!$rh || !$wh) {
            return false;
        }

        while (!feof($rh)) {
            if (fwrite($wh, fread($rh, 4096)) === FALSE) {
                return false;
            }
            //echo '#'; 
            flush();
        }

        fclose($rh);
        fclose($wh);

        return true;
    }

    private function getGUID()
    {
        if (function_exists('com_create_guid')) {
            return com_create_guid();
        } else {
            $charid = strtoupper(md5(uniqid(rand(), true)));
            $hyphen = chr(45);
            $uuid = "_"
                . substr($charid, 0, 8) . $hyphen
                . substr($charid, 8, 4) . $hyphen
                . substr($charid, 12, 4) . $hyphen
                . substr($charid, 16, 4) . $hyphen
                . substr($charid, 20, 12);
            return $uuid;
        }
    }


    public function download($file, $destination = NULL)
    {


        if (!file_exists($file)) {
            $hasContentDispoHeader = false;


            /**
             * If the file appears to be a valid URL check the HEADERS
             * the other Routes return false, could be certificate errors
             */
            if (filter_var($file, FILTER_VALIDATE_URL)) {


                $context = stream_context_create(array(
                    'http' => array('method' => 'HEAD'),
                    'ssl' => array(
                        'verify_peer' => false,
                        'verify_peer_name' => false
                    )
                ));
                $response = @fopen($file, 'rb', false, $context);
                $metadata = @stream_get_meta_data($response);

                if (count($metadata) > 0) {
                    foreach ($metadata['wrapper_data'] as $key => $value) {
                        if (stristr($value, "Content-Disposition: attachment") !== FALSE) {
                            $hasContentDispoHeader = true;
                        };
                    }
                }
            }

            if (!$hasContentDispoHeader) {
                throw new Exception("File does not exists. $file");
            }
        }

        /**
         *if empty use Random guid as destination filename.
         */
        if (is_null($destination)) {
            $destination =  $this->getGUID() . ".csv";
        }


        if ($this->StartFileStream($file, $destination . "~")) {
            echo "File download successful: $destination" . PHP_EOL;
            if (rename($destination . "~", $destination)) {
                return $destination;
            } else {
                throw new Exception("Could not rename temp file: $destination~");
            }
        }
        unlink($destination . "~");

        throw new Exception(
            "File download not successful: " . $file
        );
    }
}
