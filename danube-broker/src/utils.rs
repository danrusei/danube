use rand::Rng;

pub(crate) fn get_random_id() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen::<u64>()
}
