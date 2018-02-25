use std::str::from_utf8;
use std::process::exit;
use std::io::Read;

use hyper::header::{Headers, ContentType};
use reqwest;

pub fn get_header_string_value<'a>(name: &'a str, headers: &'a Headers) -> Option<String> {
    let header_option = headers.get_raw(name);
    let mut value = String::from("");

    match header_option {

        Some(header) => {

            for line in header.into_iter() {
                let data: &[u8] = line;

                match String::from_utf8(Vec::from(data)) {
                    Ok(field_value) => value.push_str(&field_value[..]),
                    Err(_) => error!("Error while reading header value")
                }

            }

        },
        None => {},
    }

    Some(value)
}

pub fn read_next_message(response: &mut reqwest::Response) -> String {
    let mut msg_length_str = String::from("");
    let mut buffer: String = String::from("");
    let mut have_msg_length = false;

    while !have_msg_length {
        let mut buf: Vec<u8> = vec![0; 1];

        match response.read(&mut buf[..]) {

            Ok(_) => {
                let resp_str = from_utf8(&buf).unwrap();

                for c in resp_str.chars() {
                    if !have_msg_length {
                        // We need to start reading looking for a newline character.  This string will then be the number of bytes we need to read for the next message.
                        if c == '\n' {
                            have_msg_length = true;

                        } else {
                            msg_length_str.push_str(c.to_string().as_str());
                        }

                    } else {
                        // We may have found the end-of-line and these characters are part of the message body.
                        buffer.push_str(c.to_string().as_str());
                    }

                }

            },
            Err(e) => {
                error!("{}", e);
                exit(1);
            }

        };

    }

    let msg_len = msg_length_str.parse::<usize>().unwrap() - buffer.len();
    let mut buf: Vec<u8> = vec![0; msg_len];

    match response.read_exact(&mut buf[..]) {

        Ok(_) => {

            match from_utf8(&buf) {
                Ok(value) => buffer.push_str(value),
                Err(_) => {}
            }

        },
        Err(e) => {
            error!("{}", e);
            exit(1);
        }
    }

    buffer
}