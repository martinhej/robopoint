<?php

namespace App\Controller\Api\V1;

use Sensio\Bundle\FrameworkExtraBundle\Configuration\Route;
use Sensio\Bundle\FrameworkExtraBundle\Configuration\Method;
use Symfony\Bundle\FrameworkBundle\Controller\Controller;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

/**
 * @Route ("/api/v1/messages")
 */
class MessageController extends Controller {

    /**
     * @Route("/")
     * @Method("POST")
     *
     * @param Request $request
     *
     * @return JsonResponse
     */
    public function pushAction(Request $request) {

        $messages = $request->request->all();

        return new JsonResponse([
            // @todo - this should be a list of message ids.
            'received' => $messages,
            'success' => [],
            'error' => [],
        ]);
    }

    /**
     * @Route("/{uuid}")
     * @Method("GET")
     *
     * @param $uuid
     *
     * @return JsonResponse
     */
    public function getAction($uuid) {

        return new JsonResponse([
            $uuid => ['message1', 'message2'],
        ]);
    }

}
