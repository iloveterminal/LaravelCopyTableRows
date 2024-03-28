<?php

namespace App\Jobs;

use Carbon\Carbon;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Mail;

/**
 * Job to send maintenance email to developer(s) to notify of completed jobs or required actions.
 */
class SendMaintenanceEmail implements ShouldQueue
{
    use InteractsWithQueue, Queueable, SerializesModels;

    /**
     * The email subject.
     *
     * @var string
     */
    protected $subject;

    /**
     * The email message to be sent.
     *
     * @var string
     */
    protected $message;

    /**
     * Create a new job instance.
     *
     * @param string $subject
     * @param string $message
     *
     * @return void
     */
    public function __construct(string $subject, string $message)
    {
        $this->subject = $subject;
        $this->message = $message;
    }

    /**
     * Send maintenance email to developer(s).
     *
     * @return void
     */
    public function handle()
    {
        Mail::send(
            'emails.maintenance',
            [
                'body' => $this->message,
                'currentTime' => Carbon::now(),
            ],
            function ($mail) {
                $mail->to(config('mail.developers'))
                    ->subject($this->subject);
            }
        );

        return;
    }
}