use zela_std::{CustomProcedure, JsonValue, RpcError};

pub struct EchoProcedure;
impl CustomProcedure for EchoProcedure {
	type Params = JsonValue;
	type SuccessData = JsonValue;
	type ErrorData = ();

	async fn run(params: Self::Params) -> Result<Self::SuccessData, RpcError<Self::ErrorData>> {
		Ok(params)
	}
}
zela_std::zela_custom_procedure!(EchoProcedure);
