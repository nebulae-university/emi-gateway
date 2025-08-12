'use strict'

const { of, throwError } = require("rxjs");
const { tap, map, mergeMap, catchError } = require('rxjs/operators');
const { ApolloError } = require("apollo-server");
const { CustomError, DefaultError } = require('./customError');
const RoleValidator = require('./RoleValidator');

let broker = require("../broker/BrokerFactory")();
broker = broker.secondaryBroker ? broker.secondaryBroker : broker;

const INTERNAL_SERVER_ERROR_CODE = 1;
const PERMISSION_DENIED_ERROR_CODE = 2;

const buildSuccessResponse$ = (rawRespponse) => {
    return of(rawRespponse).pipe(
        map(resp => {
            return {
                data: resp,
                result: {
                    code: 200
                }
            };
        })
    );
};

const buildErrorResponse$ = (errCode, rawRespponse) => {
    return of(rawRespponse).pipe(
        map(resp => {
            return {
                data: resp,
                result: {
                    code: errCode
                }
            };
        })
    );
};

const handleError$ = (err) => {
    return of(err).pipe(
        map(err => {
            const exception = { data: null, result: {} };
            // const isCustomError = err instanceof ApolloError;
            const isCustomError = err instanceof CustomError;
            if (!isCustomError) {
                // err = new ApolloError(err.message, 1, {name: err.name, msg: err.message});
                err = new DefaultError(err);
            }
            exception.result = {
                code: err.code,
                error: { ...err.getContent() }
            };
            return exception;
        })
    );
};


const extractBackEndResponse$ = (response) => {
    return of(response)
        .pipe(
            map(resp => {
                if (resp.result.code != 200) {
                    const err = new Error();
                    err.name = 'Error';
                    err.message = resp.result.error;
                    // this[Symbol()] = resp.result.error;
                    Error.captureStackTrace(err, 'Error');
                    throw err;
                }
                return resp.data;
            })
        );
}

/**
 * Validate user roles and send request to backend handler
 * @param {object} root root of GraphQl
 * @param {object} OperationArguments arguments for query or mutation
 * @param {object} context graphQl context
 * @param { Array } requiredRoles Roles required to use the query or mutation
 * @param {string} operationType  sample: query || mutation
 * @param {string} aggregateName sample: Vehicle, Client, FixedFile 
 * @param {string} methodName method name
 * @param {number} timeout timeout for query or mutation in milliseconds
 */
const sendToBackEndHandler$ = (root, OperationArguments, context, requiredRoles, operationType, aggregateName, methodName, timeout = 2000, contextName = 'undefined') => {
    const initTs = Date.now();
    const username = (context.authToken || {}).preferred_username;
    const type = `emigateway.graphql.${operationType}.${methodName}`;

    return RoleValidator.checkPermissions$(
        context.authToken.realm_access.roles,
        contextName,
        methodName,
        PERMISSION_DENIED_ERROR_CODE,
        "Permission denied",
        requiredRoles
    ).pipe(
        mergeMap(() =>
            broker.forwardAndGetReply$(
                aggregateName,
                type,
                { root, args: OperationArguments, jwt: context.encodedToken },
                timeout
            )
        ),
        catchError(err => handleError$(err, methodName).pipe(
            tap((e) => console.log(`${new Date().toUTCString()} - sendToBackEndHandler.ERROR: ${username} - ${aggregateName}.${type}: ${Date.now() - initTs}ms, ${JSON.stringify(e)}`))
        )),
        mergeMap(response => extractBackEndResponse$(response)),
        tap((res) => {
            const millis = Date.now() - initTs;
            const kchars = Math.round(JSON.stringify(res).length / 1000);
            console.log(`${new Date().toUTCString()}: sendToBackEndHandler: ${username} - ${aggregateName}.${type},': ${kchars}Kchar${kchars > 1000 ? '(HUGE)' : kchars > 500 ? '(BIG)' : ''}, ${millis}ms${millis > 2000 ? '(ULTRA-SLOW)' : millis > 1000 ? '(SLOW)' : millis > 300 ? '(MODERATE)' : ''} `);
        })
    );
};



module.exports = {
    buildSuccessResponse$,
    handleError$,
    buildErrorResponse$,
    sendToBackEndHandler$,
    extractBackEndResponse$
};