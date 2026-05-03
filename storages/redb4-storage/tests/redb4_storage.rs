use {
    async_trait::async_trait,
    gluesql_core::prelude::Glue,
    gluesql_redb4_storage::Redb4Storage,
    std::fs::{create_dir, remove_file},
    test_suite::*,
};

struct Redb4Tester {
    glue: Glue<Redb4Storage>,
}

#[async_trait(?Send)]
impl Tester<Redb4Storage> for Redb4Tester {
    async fn new(namespace: &str) -> Self {
        let _ = create_dir("tmp");
        let path = format!("tmp/{namespace}.redb4");
        let _ = remove_file(&path);

        let storage =
            Redb4Storage::new(path).expect("[Redb4Tester] failed to create storage");
        let glue = Glue::new(storage);

        Self { glue }
    }

    fn get_glue(&mut self) -> &mut Glue<Redb4Storage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, Redb4Tester);
generate_transaction_tests!(tokio::test, Redb4Tester);
generate_index_tests!(tokio::test, Redb4Tester);
generate_alter_table_tests!(tokio::test, Redb4Tester);
generate_alter_table_index_tests!(tokio::test, Redb4Tester);
generate_transaction_alter_table_tests!(tokio::test, Redb4Tester);
generate_transaction_index_tests!(tokio::test, Redb4Tester);
