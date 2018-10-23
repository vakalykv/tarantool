#include "memory.h"
#include "fiber.h"
#include "unit.h"
#include "swim_test_transport.h"
#include "swim_test_ev.h"

static int
main_f(va_list ap)
{
	swim_test_ev_init();
	swim_test_transport_init();
	(void) ap;
	swim_test_transport_free();
	swim_test_ev_free();
	return 0;
}

int
main()
{
	header();
	plan(1);
	ok(true, "true is true");

	memory_init();
	fiber_init(fiber_c_invoke);

	struct fiber *main_fiber = fiber_new("main", main_f);
	assert(main_fiber != NULL);
	fiber_wakeup(main_fiber);
	ev_run(loop(), 0);

	fiber_free();
	memory_free();

	int rc = check_plan();
	footer();
	return rc;
}