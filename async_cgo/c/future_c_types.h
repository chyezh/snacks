#pragma once


#ifdef __cplusplus
extern "C" {
#endif

    typedef struct future Future;

    typedef void (*Callback)(Future* future, void* callback_parameter);

#ifdef __cplusplus
}
#endif