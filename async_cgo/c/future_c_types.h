#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct future CFuture;

typedef void (*Callback)(CFuture* future, void* callback_parameter);

#ifdef __cplusplus
}
#endif