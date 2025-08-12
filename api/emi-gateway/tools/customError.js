//Every single error code
const INTERNAL_SERVER_ERROR_CODE = 1;
const PERMISSION_DENIED = 2;

/**
 * class to encapsulute diferent errors.
 */
class CustomError extends Error {
  constructor(name, method, code = INTERNAL_SERVER_ERROR_CODE , message = '') {
    super(message); 
    this.code = code;
    this.name = name;
    this.method = method;
  }

  getContent(){
    return {
      name: this.name,
      code: this.code,
      msg: this.message,      
      method: this.method
    }
  }
};

class DefaultError extends Error {
  constructor(anyError) {
    super(anyError.message)
    this.code = INTERNAL_SERVER_ERROR_CODE;
    this.name = anyError.name;
    this.msg = anyError.message;
    // this.stack = anyError.stack;
  }

  getContent() {
    return {
      code: this.code,
      name: this.name,
      msg: this.msg
    }
  }
}

module.exports =  { 
  CustomError,
  DefaultError
}