/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "util/aeron_platform.h"
#if defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
#include <io.h>
#endif

#include "aeron_windows.h"
#include "aeron_alloc.h"
#include "aeron_context.h"
#include "util/aeron_error.h"

#if defined(__clang__)
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wunused-function"
#endif

inline static const char *tmp_dir()
{
#if defined(_MSC_VER)
    static char buff[MAX_PATH + 1];

    if (GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        return buff;
    }

    return NULL;
#else
    const char *dir = "/tmp";

    if (getenv("TMPDIR"))
    {
        dir = getenv("TMPDIR");
    }

    return dir;
#endif
}

inline static bool has_file_separator_at_end(const char *path)
{
#if defined(_MSC_VER)
    const char last = path[strlen(path) - 1];
    return last == '\\' || last == '/';
#else
    return path[strlen(path) - 1] == '/';
#endif
}

#if defined(__clang__)
    #pragma clang diagnostic pop
#endif

inline static const char *username()
{
    const char *username = getenv("USER");
#if (_MSC_VER)
    if (NULL == username)
    {
        username = getenv("USERNAME");
        if (NULL == username)
        {
             username = "default";
        }
    }
#else
    if (NULL == username)
    {
        username = "default";
    }
#endif
    return username;
}

#define AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT (false)

void aeron_default_error_handler(void *clientd, int errcode, const char *message)
{
    fprintf(stderr, "ERROR: (%d): %s\n", errcode, message);
    exit(EXIT_FAILURE);
}

void aeron_default_on_new_publication(
    void *clientd, const char *channel, int32_t stream_id, int32_t session_id, int64_t correlation_id)
{
}

int aeron_context_init(aeron_context_t **context)
{
    aeron_context_t *_context = NULL;

    if (NULL == context)
    {
        errno = EINVAL;
        aeron_set_err(EINVAL, "aeron_context_init(NULL): %s", strerror(EINVAL));
        return -1;
    }

    if (aeron_alloc((void **)&_context, sizeof(aeron_context_t)) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_context->aeron_dir, AERON_MAX_PATH) < 0)
    {
        return -1;
    }

#if defined(__linux__)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "/dev/shm/aeron-%s", username());
#elif defined(_MSC_VER)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "\\", username());
#else
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s%saeron-%s", tmp_dir(), has_file_separator_at_end(tmp_dir()) ? "" : "/", username());
#endif

    _context->error_handler = aeron_default_error_handler;
    _context->error_handler_clientd = NULL;
    _context->on_new_publication = aeron_default_on_new_publication;
    _context->on_new_publication_clientd = NULL;
    _context->on_new_exclusive_publication = aeron_default_on_new_publication;
    _context->on_new_exclusive_publication_clientd = NULL;

    _context->use_conductor_agent_invoker = AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT;
    _context->agent_on_start_func = NULL;
    _context->agent_on_start_state = NULL;

    char *value = NULL;

    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s", value);
    }

    if ((_context->idle_strategy_func = aeron_idle_strategy_load(
        "sleeping",
        &_context->idle_strategy_state,
        NULL,
        "1ms")) == NULL)
    {
        return -1;
    }

    *context = _context;
    return 0;
}

int aeron_context_close(aeron_context_t *context)
{
    if (NULL != context)
    {
        aeron_free((void *)context->aeron_dir);
        aeron_free(context->idle_strategy_state);
        aeron_free(context);
    }

    return 0;
}

#define AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(r, a) \
do \
{ \
    if (NULL == (a)) \
    { \
        aeron_set_err(EINVAL, "%s", strerror(EINVAL)); \
        return (r); \
    } \
} \
while (false)

int aeron_context_set_dir(aeron_context_t *context, const char *value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    snprintf(context->aeron_dir, AERON_MAX_PATH - 1, "%s", value);
    return 0;
}

const char *aeron_context_get_dir(aeron_context_t *context)
{
    return NULL != context ? context->aeron_dir : NULL;
}

int aeron_context_set_error_handler(aeron_context_t *context, aeron_error_handler_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->error_handler = handler;
    context->error_handler_clientd = NULL;
    return 0;
}

aeron_error_handler_t aeron_context_get_error_handler(aeron_context_t *context)
{
    return NULL != context ? context->error_handler : aeron_default_error_handler;
}

void *aeron_context_get_error_handler_clientd(aeron_context_t *context)
{
    return NULL != context ? context->error_handler_clientd : NULL;
}

int aeron_context_set_on_new_publication(aeron_context_t *context, aeron_on_new_publication_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_new_publication = handler;
    context->on_new_publication_clientd = clientd;
    return 0;
}

aeron_on_new_publication_t aeron_context_get_on_new_publication(aeron_context_t *context)
{
    return NULL != context ? context->on_new_publication : aeron_default_on_new_publication;
}

void *aeron_context_get_on_new_publication_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_new_publication_clientd : NULL;
}

int aeron_context_set_on_new_exclusive_publication(
    aeron_context_t *context, aeron_on_new_publication_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_new_exclusive_publication = handler;
    context->on_new_exclusive_publication_clientd = clientd;
    return 0;
}

aeron_on_new_publication_t aeron_context_get_on_new_exclusive_publication(aeron_context_t *context)
{
    return NULL != context ? context->on_new_exclusive_publication : aeron_default_on_new_publication;
}

void *aeron_context_get_on_new_exclusive_publication_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_new_exclusive_publication_clientd : NULL;
}

int aeron_context_set_use_conductor_agent_invoker(aeron_context_t *context, bool value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->use_conductor_agent_invoker = value;
    return 0;
}

bool aeron_contest_get_use_conductor_agent_invoker(aeron_context_t *context)
{
    return NULL != context ? context->use_conductor_agent_invoker : AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT;
}
