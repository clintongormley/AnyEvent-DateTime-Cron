package AnyEvent::DateTime::Cron;

use warnings;
use strict;
use DateTime();
use DateTime::Event::Cron();
use AnyEvent();

our $VERSION = 0.01;

#===================================
sub new {
#===================================
    my $class = shift;
    return bless {
        _jobs    => {},
        _debug   => 0,
        _id      => 0,
        _running => 0,
    }, $class;
}

#===================================
sub add {
#===================================
    my $self = shift;
    my @args = ref $_[0] eq 'ARRAY' ? @{ shift() } : @_;
    while (@args) {
        my $cron = shift @args;
        my ( $cb, %params );
        while (@args) {
            my $key = shift @args;
            if ( ref $key eq 'CODE' ) {
                $cb = $key;
                last;
            }
            die "Unknown param '$key'"
                unless $key =~ /^(name|single)$/;
            $params{$key} = shift @args;
        }
        die "No callback found for cron entry '$cron'"
            unless $cb;

        my $event = DateTime::Event::Cron->new($cron);
        my $id    = ++$self->{_id};
        $params{name} ||= $id;
        my $job = $self->{_jobs}{$id} = {
            event    => $event,
            cb       => $cb,
            id       => $id,
            watchers => {},
            %params,
        };

        $self->_schedule($job)
            if $self->{_running};
    }
    return $self;
}

#===================================
sub delete {
#===================================
    my $self = shift;
    my @ids = ref $_[0] eq 'ARRAY' ? @{ $_[0] } : @_;

    for (@ids) {
        print STDERR "Deleting job '$_'\n"
            if $self->{_debug};

        if ( my $job = delete $self->{_jobs}{$_} ) {
            $job->{watchers} = {};
        }
        elsif ( $self->{_debug} ) {
            print STDERR "Job '$_' not found\n";
        }
    }
    return $self;
}

#===================================
sub start {
#===================================
    my $self = shift;
    my $cv = $self->{_cv} = AnyEvent->condvar;

    $cv->begin( sub { $self->stop } );

    $self->{_signal} = AnyEvent->signal(
        signal => 'TERM',
        cb     => sub {
            print STDERR "Shutting down\n" if $self->{_debug};
            $cv->end;
        }
    );
    $self->{_running} = 1;
    $self->_schedule( values %{ $self->{_jobs} } );

    return $cv;
}

#===================================
sub stop {
#===================================
    my $self = shift;
    $_->{watchers} = {} for values %{ $self->{_jobs} };

    my $cv = delete $self->{_cv};
    delete $self->{_signal};
    $self->{_running} = 0;

    $cv->send;
    return $self;
}

#===================================
sub _schedule {
#===================================
    my $self = shift;

    AnyEvent->now_update();
    my $now_epoch = AnyEvent->now;
    my $now       = DateTime->from_epoch( epoch => $now_epoch );
    my $debug     = $self->{_debug};

    for my $job (@_) {
        my $name       = $job->{name};
        my $next_run   = $job->{event}->next($now);
        my $next_epoch = $next_run->epoch;
        my $delay      = $next_epoch - $now_epoch;

        print STDERR "Scheduling job '$name' for: $next_run\n"
            if $debug;

        my $run_event = sub {
            print STDERR "Starting job '$name'\n"
                if $debug;

            $self->{_cv}->begin;
            delete $job->{watchers}{$next_epoch};

            $self->_schedule($job);

            if ( $job->{single} && $job->{running}++ ) {
                print STDERR "Skipping job '$name' - still running\n"
                    if $debug;
            }
            else {
                eval { $job->{cb}->( $self->{_cv}, $job ); 1 }
                    or warn $@ || 'Unknown error';
                delete $job->{running};
                print STDERR "Finished job '$name'\n"
                    if $debug;
            }

            $self->{_cv}->end;
        };

        $job->{watchers}{$next_epoch} = AnyEvent->timer(
            after => $delay,
            cb    => $run_event
        );
    }
}

#===================================
sub debug {
#===================================
    my $self = shift;
    $self->{_debug} = shift if @_;
    return $self;
}

#===================================
sub jobs { shift->{_jobs} }
#===================================

1;
__END__

=head1 NAME

AnyEvent::DateTime::Cron - AnyEvent crontab with DateTime::Event::Cron

=head1 SYNOPSIS

    AnyEvent::DateTime::Cron->new()
        ->add(
            '* * * * *'   => sub { warn "Every minute"},
            '*/2 * * * *' => sub { warn "Every second minute"},
          )
        ->start
        ->recv

    $cron = AnyEvent::DateTime::Cron->new();
    $cron->debug(1)->add(
        {
            cron => '* * * * *',
            cb   => sub {'foo'},
            name => 'job_name_for_debugging'
        },
        {...}.
    );

    $cron->delete($job_id,$job_id...)

    $cv = $cron->start;
    $cv->recv;

=head1 DESCRIPTION

L<AnyEvent::DateTime::Cron> is an L<AnyEvent> based crontab, which supports
all crontab formats recognised by L<DateTime::Event::Cron>.

It allows you to shut down a running instance gracefully, by waiting for
any running cron jobs to finish before exiting.

=head1 METHODS

=head2 new()

    $cron = AnyEvent::DateTime::Cron->new();

Creates a new L<AnyEvent::DateTime::Cron> instance - takes no parameters.

=head2 add()

    $cron->add(
        '* * * * *'     => sub {...},
        {
            cron => '* * * * *',
            cb   => sub {...},
            name => 'my_job'
        }
    );

Use C<add()> to add new cron jobs.  It accepts a list of crontab entries and
callbacks, or alternatively, a list of hashrefs with named parameters. The
latter allows you to pass in an optional C<name> parameter, which can
be useful for debugging.

Each new job is assigned an ID.

New jobs can be added before running, or while running.

See L</"CALLBACKS"> for more.

=head2 delete()

    $cron->delete($job_id,$job_id,....)

Delete one or more existing jobs, before starting or while running.

=head2 start()

    my $cv = $cron->start;
    $cv->recv;

Schedules all jobs to start at the next scheduled time, and returns an
L<AnyEvent condvar|http://metacpan.org/module/AnyEvent#CONDITION-VARIABLES>.

The cron loop can be started by calling C<recv()> on the condvar.

=head2 stop()

    $cron->stop()

Used to shutdown the cron loop gracefully. You can also shutdown the cron loop
by sending a C<HUP> signal to the process.

=head2 jobs()

    $job = $cron->jobs

Returns a hashref containing all the current cron jobs.

=head2 debug()

    $cron->debug(1|0)

Turn on debugging.

=head1 CALLBACKS

A callback is a coderef (eg an anonymous subroutine) which will be called
every time your job is triggered. Callbacks should use C<AnyEvent> themselves,
so that they run asynchronously, otherwise they can block the execution
of the cron loop, delaying other jobs.

Two parameters are passed to your callback: the main C<$cv> of the cron loop,
and the C<$job_description> which contains various details about the current
job.

The C<$cv> is the most important parameter, as it allows you to control how
your cron loop will shut down.  If your callback doesn't use
C<AnyEvent> and is blocking, then your callback will complete before it
returns to the cron loop.

However, if your callback is running asynchronously (and it really should),
then you can block the cron loop from responding to a L</"stop()"> request
until your job has completed:

    sub {
        my $cv = shift;
        $cv->begin;
        do_something_asynchronous( cb => sub { $cv->end })
    }

Callbacks are called inside an C<eval> so if they throw an error, they
will warn, but won't cause the cron loop to exit.

=head1 AUTHOR

Clinton Gormley, C<< <drtech at cpan.org> >>

=head1 BUGS

If you have any suggestions for improvements, or find any bugs, please report
them to L<http://github.com/clintongormley/AnyEvent-DateTime-Cron/issues>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc AnyEvent::DateTime::Cron

You can also look for information at
L<http://github.com/clintongormley/AnyEvent-DateTime-Cron>


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Clinton Gormley.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;
