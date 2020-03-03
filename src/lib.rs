use mysql_async::prelude::Queryable;
use mysql_async::{from_row, params, Conn, Pool};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

const URL: &str = "mysql://justus:@localhost:3306/olmmcc";

pub struct Session {
    id: String,
    pool: Pool,
}

async fn garbage_collector(conn: Conn, days: u32, prob: u32) {
    if rand::thread_rng().gen_range(0, prob) == 0 {
        let query = format!(
            "DELETE FROM sessions WHERE timestamp < now() - INTERVAL {} day",
            days
        );
        conn.prep_exec(query, ()).await.unwrap();
    }
}

impl Session {
    pub async fn new(days: u32, prob: u32) -> Self {
        let pool = Pool::new(URL);
        let garbage_conn = pool.get_conn().await.unwrap();
        garbage_collector(garbage_conn, days, prob).await;
        let conn = pool.get_conn().await.unwrap();
        let id = thread_rng().sample_iter(&Alphanumeric).take(255).collect();
        conn.prep_exec(
            "INSERT INTO sessions (timestamp, id, data) VALUES (now() + 0, :id, \"{}\")",
            params!("id" => &id),
        )
        .await
        .unwrap();
        Session { id, pool }
    }
    pub async fn from_id(id: &str) -> Option<Self> {
        let pool = Pool::new(URL);
        let conn = pool.get_conn().await.unwrap();
        let result = conn
            .prep_exec(
                "SELECT EXISTS(SELECT * FROM sessions WHERE id = :id)",
                params!(id),
            )
            .await
            .unwrap();
        let (_, collected_result) = result.collect::<bool>().await.unwrap();
        if collected_result[0] {
            Some(Session {
                id: id.to_string(),
                pool,
            })
        } else {
            None
        }
    }
    pub fn get_id(&self) -> &str {
        &self.id
    }
    pub async fn get(&mut self, key: &str) -> Option<String> {
        let conn = self.pool.get_conn().await.unwrap();
        let query = format!(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.{}')) FROM sessions WHERE id = :id",
            key
        );
        match conn.prep_exec(query, params!("id" => &self.id)).await {
            Ok(t) => {
                let (_, u) = t
                    .map_and_drop(|i| from_row::<Option<String>>(i))
                    .await
                    .unwrap();
                u[0].clone()
            }
            _ => None,
        }
    }
    pub async fn set(&mut self, key: &str, value: String) -> &mut Self {
        let conn = self.pool.get_conn().await.unwrap();
        let query = format!(
            "UPDATE sessions SET data = JSON_SET(`data`, '$.{}', :value) WHERE id = :id",
            key
        );
        conn.prep_exec(query, params!(value, "id" => &self.id))
            .await
            .unwrap();
        self
    }
    pub async fn unset(&mut self, key: &str) -> &mut Self {
        let conn = self.pool.get_conn().await.unwrap();
        let query = format!(
            "UPDATE sessions SET data = JSON_REMOVE(`data`, '$.{}') WHERE id = :id",
            key
        );
        conn.prep_exec(query, params!("id" => &self.id))
            .await
            .unwrap();
        self
    }
    pub async fn clear(&mut self) -> &mut Self {
        let conn = self.pool.get_conn().await.unwrap();
        conn.prep_exec(
            "UPDATE sessions SET data = JSON_OBJECT() WHERE id = :id",
            params!("id" => &self.id),
        )
        .await
        .unwrap();
        self
    }
    pub async fn delete(&mut self) {
        let conn = self.pool.get_conn().await.unwrap();
        conn.prep_exec(
            "DELETE FROM sessions WHERE id = :id",
            params!("id" => &self.id),
        )
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn session_test() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session = super::Session::new(30, 100).await;
            let id = session.get_id().to_string();
            session.set("no", "on".to_string()).await;
            session.clear().await;
            assert_eq!(session.get("no").await, None);
            let mut other_session = super::Session::from_id(&id).await.unwrap();
            other_session.set("on", "no".to_string()).await;
            assert_eq!(session.get("on").await, other_session.get("on").await);
            assert_eq!(session.get("on").await.unwrap(), "no");
            assert_eq!(session.unset("on").await.get("on").await, None);
            session.delete().await;
            if let Some(_) = super::Session::from_id(&id).await {
                panic!("Delete failed!");
            }
        });
    }
}
