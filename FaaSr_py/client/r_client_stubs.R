library(httr)

faasr_log <- function(log_message) {
    request_json <- list(
        "ProcedureID" = "faasr_log",
        Arguments = list(
            "log_message" = log_message
        )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)

    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Success)
    } else {
        err_msg <- "Request to FaaSr RPC failed"
        faasr_exit(error=TRUE, message=err_msg)
        quit(status = 1, save = "no")
    }
}


faasr_put_file <- function(local_file, remote_file, server_name="", local_folder=".", remote_folder=".") {
    request_json <- list(
        "ProcedureID" = "faasr_put_file",
        "Arguments" = list("local_file" = local_file, 
                    "remote_file" = remote_file,
                    "server_name" = server_name,
                    "local_folder" = local_folder,
                    "remote_folder" = remote_folder
        )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)

    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Success)
    } else {
        err_msg <- "Request to FaaSr RPC failed"
        faasr_exit(error=TRUE, message=err_msg)
        quit(status = 1, save = "no")
    }
}
    

faasr_get_file <- function(local_file, remote_file, server_name="", local_folder=".", remote_folder=".") {
    request_json <- list(
        "ProcedureID" = "faasr_get_file",
        "Arguments" = list ("local_file" = local_file, 
                    "remote_file" = remote_file,
                    "server_name" = server_name,
                    "local_folder" = local_folder,
                    "remote_folder" = remote_folder
        )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)

    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Success)
    } else {
        err_msg <- "Request to FaaSr RPC failed"
        faasr_exit(error=TRUE, message=err_msg)
        quit(status = 1, save = "no")
    }
}

faasr_delete_file <- function(remote_file, server_name="", remote_folder="") {
    request_json <- list(
        "ProcedureID" = "faasr_delete_file",
        "Arguments" = list("remote_file" = remote_file, 
                    "server_name" = server_name,
                    "remote_folder" = remote_folder
        )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)

    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Success)
    } else {
        err_msg <- "Request to FaaSr RPC failed"
        faasr_exit(error=TRUE, message=err_msg)
        quit(status = 1, save = "no")
    }
}


faasr_get_folder_list <- function(server_name="", prefix = "") {
    request_json <- list(
        "ProcedureID" = "faasr_get_folder_list",
        "Arguments" = list("server_name" = server_name,
                     "prefix" = prefix
                     )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)
    
    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Data$folder_list)
    } else {
        err_msg <- "Failed to get folder list"
        faasr_exit(error=TRUE, message=err_msg)
        quit(status = 1, save = "no")
    }
}


faasr_rank <- function(rank_value=NULL) {
    rank_json <- list(
        Rank = rank_value
    )
    r <- POST("http://127.0.0.1:8000/faasr-return", body=rank_json, encode="json")
    response_content <- content(r)
    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Success)
    } else {
        err_msg <- "Request to FaaSr RPC failed"
        faasr_exit(error=TRUE, message=err_msg)
        quit(status = 1, save = "no")
    }
}


faasr_return <- function(return_value=NULL) {
    return_json <- list(
        FunctionResult = return_value
    )
    r <- POST("http://127.0.0.1:8000/faasr-return", body=return_json, encode="json")
    if (!is.null(r$status_code) && r$status_code == 200) {
        response_content <- content(r)
        if (!is.null(response_content$Success) && response_content$Success) {
            quit(status = 0, save = "no")
        } else {
            err_msg <- "Request to FaaSr RPC failed"
            message(err_msg)
            faasr_exit(error=TRUE)
        }
    } else {
        err_msg <- paste0("HTTP request failed with status code: ", r$status_code)
        faasr_exit(error=TRUE, message=err_msg)
        quit(status = 1, save = "no")
    }
}


faasr_exit <- function(message=NULL, error=TRUE) {
    exit_json <- list(
        Error = error,
        Message = message
    )
    r <- POST("http://127.0.0.1:8000/faasr-exit", body=exit_json, encode="json")
    response_content <- content(r)
    if (!is.null(response_content$Success) && response_content$Success) {
        quit(status = 0, save = "no")
    } else {
        err_msg <- "Request to FaaSr RPC failed"
        message(err_msg)
        quit(status = 1, save = "no")
    }
}