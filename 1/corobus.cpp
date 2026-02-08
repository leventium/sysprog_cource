#include "corobus.h"
#include "libcoro.h"

#include <assert.h>
#include <cstddef>
#include <stdlib.h>
#include <string.h>
#include <queue>
#include <vector>

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	std::queue<struct coro*> send_queue;
	/** Coroutines waiting until the channel is not empty. */
	std::queue<struct coro*> recv_queue;
	/** Message queue. */
	std::queue<unsigned> messages;
};

struct coro_bus {
	std::vector<struct coro_bus_channel*> channels;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

static void
wakeup_coro_queue(std::queue<struct coro*>& coro_queue) {
    for (; !coro_queue.empty(); coro_queue.pop()) {
        coro_wakeup(coro_queue.front());
    }
}

static void
suspend_this_and_save_to(std::queue<struct coro*>& coro_queue) {
    struct coro* this_coro = coro_this();
    coro_queue.push(this_coro);
    coro_suspend();
}

static void
wakeup_first_and_remove_from(std::queue<struct coro*>& coro_queue) {
    if (!coro_queue.empty()) {
        struct coro* waiting_coro = coro_queue.front();
        coro_queue.pop();
        coro_wakeup(waiting_coro);
    }
}

static struct coro_bus_channel*
get_chanel_from(struct coro_bus* bus, int channel) {
    if (channel < 0) {
        return NULL;
    }
    if (bus->channels.size() <= (size_t)channel) {
        return NULL;
    }
    return bus->channels[channel];
}

struct coro_bus *
coro_bus_new(void)
{
    struct coro_bus* res = new struct coro_bus;
	return res;
}

void
coro_bus_delete(struct coro_bus *bus)
{
    for (size_t i = 0; i < bus->channels.size(); ++i) {
        if (bus->channels[i] != NULL) {
            coro_bus_channel_close(bus, i);
        }
    }

    delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
    int chan_desc = -1;
    for (size_t i = 0; i < bus->channels.size(); ++i) {
        chan_desc = (bus->channels[i] == NULL) ? i : chan_desc;
    }
    if (chan_desc == -1) {
        bus->channels.push_back(NULL);
        chan_desc = bus->channels.size() - 1;
    }

    bus->channels[chan_desc] = new struct coro_bus_channel;
    struct coro_bus_channel* new_chan = bus->channels[chan_desc];
    new_chan->size_limit = size_limit;

    return chan_desc;

	/*
	 * One of the tests will force you to reuse the channel
	 * descriptors. It means, that if your maximal channel
	 * descriptor is N, and you have any free descriptor in
	 * the range 0-N, then you should open the new channel on
	 * that old descriptor.
	 *
	 * A more precise instruction - check if any of the
	 * bus->channels[i] with i = 0 -> bus->channel_count is
	 * free (== NULL). If yes - reuse the slot. Don't grow the
	 * bus->channels array, when have space in it.
	 */
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
    struct coro_bus_channel* chan = get_chanel_from(bus, channel);
    if (chan == NULL) {
        return;
    }

    wakeup_coro_queue(chan->send_queue);
    wakeup_coro_queue(chan->recv_queue);
    delete chan;
    bus->channels[channel] = NULL;
    coro_yield();

	/*
	 * Be very attentive here. What happens, if the channel is
	 * closed while there are coroutines waiting on it? For
	 * example, the channel was empty, and some coros were
	 * waiting on its recv_queue.
	 *
	 * If you wakeup those coroutines and just delete the
	 * channel right away, then those waiting coroutines might
	 * on wakeup try to reference invalid memory.
	 *
	 * Can happen, for example, if you use an intrusive list
	 * (rlist), delete the list itself (by deleting the
	 * channel), and then the coroutines on wakeup would try
	 * to remove themselves from the already destroyed list.
	 *
	 * Think how you could address that. Remove all the
	 * waiters from the list before freeing it? Yield this
	 * coroutine after waking up the waiters but before
	 * freeing the channel, so the waiters could safely leave?
	 */
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
    for (;;) {
        if (coro_bus_try_send(bus, channel, data) == 0) {
            break;
        }

        enum coro_bus_error_code err = coro_bus_errno();
        if (err == CORO_BUS_ERR_NO_CHANNEL) {
            return -1;
        } else if (err == CORO_BUS_ERR_WOULD_BLOCK) {
            struct coro_bus_channel* chan = get_chanel_from(bus, channel);
            suspend_this_and_save_to(chan->send_queue);
        } else {
            coro_bus_errno_set(CORO_BUS_ERR_NOT_IMPLEMENTED);
            return -1;
        }
    }

    return 0;

	/*
	 * Try sending in a loop, until success. If error, then
	 * check which one is that. If 'wouldblock', then suspend
	 * this coroutine and try again when woken up.
	 *
	 * If see the channel has space, then wakeup the first
	 * coro in the send-queue. That is needed so when there is
	 * enough space for many messages, and many coroutines are
	 * waiting, they would then wake each other up one by one
	 * as lone as there is still space.
	 */
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
    struct coro_bus_channel* chan = get_chanel_from(bus, channel);
    if (chan == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    if (chan->messages.size() >= chan->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    chan->messages.push(data);
    wakeup_first_and_remove_from(chan->recv_queue);
    return 0;

	/*
	 * Append data if has space. Otherwise 'wouldblock' error.
	 * Wakeup the first coro in the recv-queue! To let it know
	 * there is data.
	 */
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    for (;;) {
        if (coro_bus_try_recv(bus, channel, data) == 0) {
            break;
        }

        enum coro_bus_error_code err = coro_bus_errno();
        if (err == CORO_BUS_ERR_NO_CHANNEL) {
            return -1;
        } else if (err == CORO_BUS_ERR_WOULD_BLOCK) {
            struct coro_bus_channel* chan = get_chanel_from(bus, channel);
            suspend_this_and_save_to(chan->recv_queue);
        } else {
            coro_bus_errno_set(CORO_BUS_ERR_NOT_IMPLEMENTED);
            return -1;
        }
    }

    return 0;
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    struct coro_bus_channel* chan = get_chanel_from(bus, channel);

    if (chan == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    if (chan->messages.empty()) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    *data = chan->messages.front();
    chan->messages.pop();
    wakeup_first_and_remove_from(chan->send_queue);
    return 0;
}


#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	/* IMPLEMENT THIS FUNCTION */
	(void)bus;
	(void)data;
	coro_bus_errno_set(CORO_BUS_ERR_NOT_IMPLEMENTED);
	return -1;
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	/* IMPLEMENT THIS FUNCTION */
	(void)bus;
	(void)data;
	coro_bus_errno_set(CORO_BUS_ERR_NOT_IMPLEMENTED);
	return -1;
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	/* IMPLEMENT THIS FUNCTION */
	(void)bus;
	(void)channel;
	(void)data;
	(void)count;
	coro_bus_errno_set(CORO_BUS_ERR_NOT_IMPLEMENTED);
	return -1;
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	/* IMPLEMENT THIS FUNCTION */
	(void)bus;
	(void)channel;
	(void)data;
	(void)count;
	coro_bus_errno_set(CORO_BUS_ERR_NOT_IMPLEMENTED);
	return -1;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	/* IMPLEMENT THIS FUNCTION */
	(void)bus;
	(void)channel;
	(void)data;
	(void)capacity;
	coro_bus_errno_set(CORO_BUS_ERR_NOT_IMPLEMENTED);
	return -1;
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	/* IMPLEMENT THIS FUNCTION */
	(void)bus;
	(void)channel;
	(void)data;
	(void)capacity;
	coro_bus_errno_set(CORO_BUS_ERR_NOT_IMPLEMENTED);
	return -1;
}

#endif
