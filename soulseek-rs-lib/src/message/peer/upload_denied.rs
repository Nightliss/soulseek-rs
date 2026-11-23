use crate::info;
use crate::{
    message::{Message, MessageHandler},
    peer::PeerMessage,
};
use std::sync::mpsc::Sender;

pub struct UploadDeniedHandler;
impl MessageHandler<PeerMessage> for UploadDeniedHandler {
    fn get_code(&self) -> u32 {
        50
    }
    fn handle(&self, message: &mut Message, sender: Sender<PeerMessage>) {
        let filename = message.read_string();
        let reason = message.read_string();
        info!("Upload denied for {}: {}", filename, reason);
        if reason == "Queued" {
            return;
        }
        let _ = sender.send(PeerMessage::UploadFailed(String::new(), filename));
    }
}

/// Build an `UploadDenied` (peer code 50) for `filename`.
#[must_use]
pub fn build_upload_denied(filename: &str, reason: &str) -> Message {
    Message::new()
        .write_int32(50)
        .write_string(filename)
        .write_string(reason)
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::framed;

    #[test]
    fn a_queued_reason_is_not_a_failure() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut message = framed(|m| {
            m.write_string("@@share\\gone.mp3").write_string("Queued");
        });

        UploadDeniedHandler.handle(&mut message, tx);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn a_built_denial_carries_filename_and_reason() {
        let mut message = build_upload_denied("@@share\\f.mp3", "Cancelled");
        message.set_pointer(4);
        assert_eq!(message.read_string(), "@@share\\f.mp3");
        assert_eq!(message.read_string(), "Cancelled");
    }

    #[test]
    fn the_denied_filename_is_forwarded() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut message = framed(|m| {
            m.write_string("@@share\\gone.mp3")
                .write_string("File not shared.");
        });

        UploadDeniedHandler.handle(&mut message, tx);
        match rx.try_recv() {
            Ok(PeerMessage::UploadFailed(_, filename)) => {
                assert_eq!(filename, "@@share\\gone.mp3");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
