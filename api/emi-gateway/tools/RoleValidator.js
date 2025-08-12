const { map, switchMap, delay, toArray, mergeMap } = require('rxjs/operators');
const { of, BehaviorSubject, from, throwError } = require('rxjs');
const { CustomError } = require("./customError");
/**
 * Role validator
 */
class RoleValidator {


  /**
   * Checks if the user has the permissions needed, otherwise throws an error according to the passed parameters.
   * @param {*} userRoles Roles of the user
   * @param {*} requiredRoles required roles
   * @param {*} errorName Name of the error that will be thrown if the user does not have at least one of the required roles
   * @param {*} errorMethodName Method Name where the error was generated.
   * @param {*} errorCode Error code that will be thrown if the user do not have at least one of the required roles
   * @param {*} errorMessage Error message that will be thrown if the user do not have at least one of the required roles
   */
  static checkAndThrowError(userRoles, requiredRoles, errorName, errorMethodName, errorCode, errorMessage){
    if(!RoleValidator.hasPermissions(userRoles, requiredRoles)){
      const err = new CustomError(errorName, errorMethodName, errorCode, errorMessage);
      err.message = err.getContent();
      Error.captureStackTrace(err, "Error");
      throw err;
    }
  }

/**
 * Observable that checks if the user has the permissions needed, otherwise throws an error according to the passed parameters.
 *
 * @param {*} UserRoles Roles of the authenticated user
 * @param {*} name Context name
 * @param {*} errorMethodName Method Name where the error was generated.
 * @param {*} errorCode  This is the error code that will be thrown if the user do not have the required roles
 * @param {*} errorMessage This is the error message that will be used if the user do not have the required roles
 * @param {*} requiredRoles Array with required roles (The authenticated user must have at least one of the required roles,
 *  otherwise the operation that the user is trying to do will be rejected.
 */
static checkPermissions$(
    userRoles,
    contextName,
    method,
    errorCode,
    errorMessage,
    requiredRoles
  ) {
    if (requiredRoles == undefined || requiredRoles.length == 0) {
      return of([]);
    }

    return from(requiredRoles)
    .pipe(
      map(requiredRole => !( userRoles == undefined || userRoles.length == 0 || !userRoles.includes(requiredRole)) ),
      toArray(),
      mergeMap( validRoles => (!validRoles.includes(true)) 
        ? throwError( new CustomError(contextName, method, errorCode, errorMessage) )
        : of(validRoles)
      )
    )
  }
  
  /**
   * Returns true if the user has at least one of the required roles
   * @param {*} userRoles Roles of the user
   * @param {*} requiredRoles Required roles
   */
  static hasPermissions(
    userRoles,
    requiredRoles
  ) {
    if(requiredRoles == undefined || requiredRoles.length == 0){
      return true;
    }
  
    if (userRoles == undefined || userRoles.length == 0) {
      return false;
    }
  
    let found = false;
    for (const requiredRole in requiredRoles) {
      
      if (userRoles.includes(requiredRoles[requiredRole])) {
        found = true;
        break;
      }
    }
  
    return found;
  }

};
  
  module.exports = RoleValidator;