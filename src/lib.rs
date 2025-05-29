use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub mod io;

pub trait TGasData: Serialize + DeserializeOwned {
    fn obj_bytes(&self) -> usize {
        size_of_val(self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchGasData<T>(pub Vec<T>);

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
