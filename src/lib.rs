use mysql::{from_value, params, Conn, OptsBuilder, Row};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

pub struct Session {
    id: String,
    conn: Conn,
}

fn new_conn() -> Conn {
    let mut builder = OptsBuilder::new();
    builder
        .db_name(Some("olmmcc"))
        .user(Some("justus"))
        .pass(Some(""));
    Conn::new(builder).unwrap()
}

fn garbage_collector(conn: &mut Conn, days: u32, prob: u32) {
    if rand::thread_rng().gen_range(0, prob) == 0 {
        conn.prep_exec(
            format!(
                "DELETE FROM sessions WHERE timestamp < now() - INTERVAL {} day",
                days
            ),
            (),
        )
        .unwrap();
    }
}

impl Session {
    pub fn new(days: u32, prob: u32) -> Self {
        let mut conn = new_conn();
        garbage_collector(&mut conn, days, prob);
        let id = thread_rng().sample_iter(&Alphanumeric).take(255).collect();
        conn.prep_exec(
            "INSERT INTO sessions (timestamp, id, data) VALUES (now() + 0, :id, \"{}\")",
            params!("id" => &id),
        )
        .unwrap();
        Session { id, conn: conn }
    }
    pub fn from_id(id: &str) -> Option<Self> {
        let mut conn = new_conn();
        if from_value(
            conn.prep_exec("SELECT EXISTS(SELECT * FROM sessions WHERE id = :id)", params!(id))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()[0]
                .clone(),
        ) {
            Some(Session {
                id: id.to_string(),
                conn,
            })
        } else {
            None
        }
    }
    pub fn get_id(&self) -> &str {
        &self.id
    }
    pub fn get(&mut self, key: &str) -> Option<String> {
        match self.conn.prep_exec(
            format!(
                "SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.{}')) FROM sessions WHERE id = :id",
                key
            ),
            params!("id" => &self.id),
        ) {
            Ok(t) => {
                let row: Vec<Row> = t.take(1).map(|x| x.unwrap()).collect();
                from_value(row[0][0].clone())
            }
            Err(_) => None,
        }
    }
    pub fn set(&mut self, key: &str, value: String) -> &mut Self {
        self.conn
            .prep_exec(
                format!(
                    "UPDATE sessions SET data = JSON_SET(`data`, '$.{}', :value) WHERE id = :id",
                    key
                ),
                params!(value, "id" => &self.id),
            )
            .unwrap();
        self
    }
    pub fn unset(&mut self, key: &str) -> &mut Self {
        self.conn
            .prep_exec(
                format!(
                    "UPDATE sessions SET data = JSON_REMOVE(`data`, '$.{}') WHERE id = :id",
                    key
                ),
                params!("id" => &self.id),
            )
            .unwrap();
        self
    }
    pub fn clear(&mut self) -> &mut Self {
        self.conn
            .prep_exec(
                format!("UPDATE sessions SET data = \"\" WHERE id = :id"), 
                params!("id" => &self.id)
            ).unwrap();
        self
    }
    pub fn delete(&mut self) {
        self.conn
            .prep_exec(
                "DELETE FROM sessions WHERE id = :id",
                params!("id" => &self.id),
            )
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn session_test() {
        let mut session = super::Session::new(30, 100);
        let id = session.get_id().to_string();
        let mut other_session = super::Session::from_id(&id).unwrap();
        other_session.set("on", "no".to_string());
        assert_eq!(session.get("on"), other_session.get("on"));
        assert_eq!(session.get("on").unwrap(), "no");
        assert_eq!(session.unset("on").get("on"), None);
        session.delete();
        if let Some(_) = super::Session::from_id(&id) {
            panic!("Delete failed!");
        }
    }
}
